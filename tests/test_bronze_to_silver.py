from datetime import datetime

from pyspark.sql import Row

from hospital_pipeline.transform import bronze_to_silver as b2s

INGESTED = datetime(2026, 7, 22, 12, 0)


def _hospital(**overrides):
    base = {
        "facility_id": "050001", "facility_name": "  MERCY GENERAL HOSPITAL ", "address": "1 MAIN ST",
        "citytown": "SACRAMENTO", "state": "CA", "zip_code": "958", "countyparish": "SACRAMENTO",
        "telephone_number": "(916) 555-0100", "hospital_type": "Acute Care Hospitals",
        "hospital_ownership": "Proprietary", "emergency_services": "Yes",
        "meets_criteria_for_birthing_friendly_designation": "Y",
        "hospital_overall_rating": "4", "hospital_overall_rating_footnote": "",
        "release_date": "2026-07-22", "_ingested_at": INGESTED,
    }
    for grp in ("mort", "safety", "readm"):
        for kind in ("better", "no_different", "worse"):
            base[f"count_of_{grp}_measures_{kind}"] = "Not Available"
    return base | overrides


def test_hospital_cleaning_types_and_sentinels(spark):
    df = spark.createDataFrame([Row(**_hospital()), Row(**_hospital(facility_id="050002",
                                                                     hospital_overall_rating="Not Available"))])
    good, bad = b2s.transform_hospitals(df)
    rows = {r.facility_id: r for r in good.collect()}

    assert bad.count() == 0
    r = rows["050001"]
    assert r.facility_name == "Mercy General Hospital"   # trimmed + title-cased
    assert r.zip_code == "00958"                          # left-padded
    assert r.overall_rating == 4
    assert r.has_emergency_services is True and r.is_birthing_friendly is True
    assert r.mort_measures_better is None                 # "Not Available" -> NULL
    assert rows["050002"].overall_rating is None


def test_invalid_rows_are_quarantined_with_reasons(spark):
    df = spark.createDataFrame([
        Row(**_hospital()),
        Row(**_hospital(facility_id="12AB")),
        Row(**_hospital(facility_id="050003", state="ZZ")),
        Row(**_hospital(facility_id="", state="ZZ")),
    ])
    good, bad = b2s.transform_hospitals(df)
    reasons = {r.facility_id: r._reject_reason for r in bad.collect()}
    assert good.count() == 1
    assert reasons["12AB"] == "invalid_facility_id"
    assert reasons["050003"] == "invalid_state"
    assert set(reasons[None].split(";")) == {"invalid_facility_id", "invalid_state"}  # "" nulled first


def test_measure_parsing_is_ansi_safe(spark):
    base = {"facility_id": "050001", "state": "CA", "measure_id": "MORT_30_HF", "measure_name": "HF mortality",
            "compared_to_national": "No Different Than the National Rate", "denominator": "1,204",
            "score": "11.6", "lower_estimate": "10.1", "higher_estimate": "13.0", "footnote": "",
            "start_date": "07/01/2022", "end_date": "06/30/2025", "release_date": "2026-07-22",
            "_ingested_at": INGESTED}
    df = spark.createDataFrame([Row(**base), Row(**(base | {"measure_id": "MORT_30_AMI", "score": "12..4",
                                                            "end_date": "not a date"}))])
    out, bad, unparseable = b2s.transform_measures(df, "complications_deaths")
    rows = {r.measure_id: r for r in out.collect()}

    assert rows["MORT_30_HF"].denominator == 1204.0      # thousands separator handled
    assert str(rows["MORT_30_HF"].period_end) == "2025-06-30"
    assert rows["MORT_30_AMI"].score is None             # bad numeric -> NULL, no crash
    assert rows["MORT_30_AMI"].period_end is None
    assert unparseable == 1
    assert rows["MORT_30_HF"].number_of_patients is None  # column absent in this feed -> added as NULL


def test_dedupe_keeps_latest_ingestion(spark):
    df = spark.createDataFrame([
        Row(facility_id="1", release_date="r", v="old", _ingested_at=datetime(2026, 1, 1)),
        Row(facility_id="1", release_date="r", v="new", _ingested_at=datetime(2026, 2, 1)),
        Row(facility_id="2", release_date="r", v="only", _ingested_at=datetime(2026, 1, 1)),
    ])
    out = {r.facility_id: r.v for r in b2s.dedupe(df, ["facility_id", "release_date"]).collect()}
    assert out == {"1": "new", "2": "only"}
