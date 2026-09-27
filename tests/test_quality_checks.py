import pandas as pd
import pytest

from hospital_pipeline.quality import checks


def _write_silver(data_dir, hospitals: pd.DataFrame, measures: pd.DataFrame, hcahps: pd.DataFrame):
    for name, df in (("hospitals", hospitals), ("measure_scores", measures), ("hcahps", hcahps)):
        for rel, part in df.groupby("release_date"):
            d = data_dir / "silver" / name / f"release_date={rel}"
            d.mkdir(parents=True, exist_ok=True)
            part.drop(columns="release_date").to_parquet(d / "part-0.parquet", index=False)


def _frames(dup: bool = False, bad_rating: bool = False):
    hospitals = pd.DataFrame({
        "facility_id": ["050001", "050002"] + (["050001"] if dup else []),
        "facility_name": ["A", "B"] + (["A"] if dup else []),
        "state": "CA", "hospital_type": "Acute Care Hospitals", "ownership": "Proprietary",
        "overall_rating": [4, 9 if bad_rating else 3] + ([4] if dup else []),
        "has_emergency_services": True, "release_date": "2026-07-22",
    })
    measures = pd.DataFrame({
        "facility_id": ["050001", "050002"], "measure_id": "MORT_30_HF",
        "compared_to_national": "No Different Than the National Rate", "score": [11.0, 12.0],
        "lower_estimate": [10.0, 11.0], "higher_estimate": [12.0, 13.0],
        "period_start": pd.Timestamp("2022-07-01"), "period_end": pd.Timestamp("2025-06-30"),
        "source_dataset": "complications_deaths", "release_date": "2026-07-22",
    })
    hcahps = pd.DataFrame({
        "facility_id": ["050001"], "measure_id": ["H_STAR_RATING"], "star_rating": [4],
        "answer_percent": [None], "completed_surveys": [300], "release_date": ["2026-07-22"],
    })
    return hospitals, measures, hcahps


def _by_name(results):
    return {(r["dataset"], r["check_name"]): r for r in results}


def test_clean_data_passes_all_blocking_checks(data_dir):
    _write_silver(data_dir, *_frames())
    results = checks.run_checks()
    checks.enforce(results)  # does not raise
    assert _by_name(results)[("hospitals", "schema_contract")]["passed"]


def test_duplicate_keys_and_out_of_range_values_block_the_pipeline(data_dir):
    _write_silver(data_dir, *_frames(dup=True, bad_rating=True))
    results = _by_name(checks.run_checks())
    assert not results[("hospitals", "business_key_unique")]["passed"]
    assert not results[("hospitals", "overall_rating_in_range")]["passed"]
    with pytest.raises(checks.DataQualityError, match="business_key_unique"):
        checks.enforce(list(results.values()))


def test_warnings_do_not_block(data_dir):
    _write_silver(data_dir, *_frames())
    stats = [{"table": "hospitals<-hospital_info", "rows_in": 100, "quarantined": 3, "duplicates_removed": 0}]
    results = checks.run_checks(stats)
    q = _by_name(results)[("hospitals<-hospital_info", "quarantine_rate_pct")]
    assert not q["passed"] and q["severity"] == "warn"
    checks.enforce(results)  # warn-level failures are recorded, not raised
