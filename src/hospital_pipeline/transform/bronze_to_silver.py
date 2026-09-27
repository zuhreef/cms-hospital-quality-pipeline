"""PySpark job: bronze (raw strings) -> silver (typed, validated, deduplicated).

Responsibilities of the silver layer:
* standardise CMS sentinel values ("Not Available", "Not Applicable", "--" ...) to NULL
* cast to proper types with ANSI-safe ``try_*`` functions (bad values -> NULL, never a crash)
* reject rows that break key rules into a quarantine area with a reason
* remove duplicate records on the business key
* conform the two measure feeds (complications/deaths + unplanned visits) into one table

Processing is incremental: only bronze release partitions that are not yet in
silver are read, and dynamic partition overwrite makes re-runs idempotent.
"""

from __future__ import annotations

import logging
from dataclasses import asdict, dataclass
from pathlib import Path

from pyspark.sql import DataFrame, SparkSession, Window
from pyspark.sql import functions as F

from hospital_pipeline.config import get_settings
from hospital_pipeline.ingest.sample_data import STATE_CODES
from hospital_pipeline.transform.spark_session import get_spark

log = logging.getLogger(__name__)

NULL_TOKENS = ["", "Not Available", "Not Applicable", "N/A", "NA", "--", "null"]
VALID_STATES = sorted(set(STATE_CODES) | {"GU", "VI", "AS", "MP"})
CCN_PATTERN = r"^[0-9A-Z]{6}$"

# silver table -> bronze datasets that feed it
SILVER_SOURCES = {
    "hospitals": ["hospital_info"],
    "measure_scores": ["complications_deaths", "unplanned_visits"],
    "hcahps": ["hcahps"],
}


@dataclass
class TableStats:
    table: str
    releases: list[str]
    rows_in: int = 0
    rows_out: int = 0
    quarantined: int = 0
    duplicates_removed: int = 0
    unparseable_numerics: int = 0


# --------------------------------------------------------------------------- helpers
def nullify_sentinels(df: DataFrame) -> DataFrame:
    """Trim every string column and map CMS sentinel tokens to NULL."""
    exprs = []
    for name, dtype in df.dtypes:
        if dtype == "string" and not name.startswith("_"):
            c = F.trim(F.col(name))
            exprs.append(F.when(c.isin(NULL_TOKENS), None).otherwise(c).alias(name))
        else:
            exprs.append(F.col(name))
    return df.select(*exprs)


def try_double(col: str) -> F.Column:
    return F.expr(f"try_cast(replace(`{col}`, ',', '') as double)")


def try_int(col: str) -> F.Column:
    return F.expr(f"try_cast(try_cast(replace(`{col}`, ',', '') as double) as int)")


def try_date(col: str) -> F.Column:
    return F.to_date(F.expr(f"try_to_timestamp(`{col}`, 'MM/dd/yyyy')"))


def yes_no(col: str) -> F.Column:
    c = F.upper(F.col(col))
    return F.when(c.isin("YES", "Y", "TRUE"), True).when(c.isin("NO", "N", "FALSE"), False)


def split_valid(df: DataFrame, rules: dict[str, F.Column]) -> tuple[DataFrame, DataFrame]:
    """Evaluate named rules; rows failing any rule go to quarantine with the reasons."""
    reasons = F.concat_ws(";", *[F.when(~F.coalesce(cond, F.lit(False)), F.lit(name))
                                 for name, cond in rules.items()])
    flagged = df.withColumn("_reject_reason", reasons)
    good = flagged.filter(F.col("_reject_reason") == "").drop("_reject_reason")
    bad = flagged.filter(F.col("_reject_reason") != "")
    return good, bad


def dedupe(df: DataFrame, keys: list[str]) -> DataFrame:
    w = Window.partitionBy(*keys).orderBy(F.col("_ingested_at").desc())
    return df.withColumn("_rn", F.row_number().over(w)).filter("_rn = 1").drop("_rn")


def pending_releases(dataset: str, silver_table: str, full_refresh: bool) -> list[str]:
    s = get_settings()
    bronze = {p.name.split("=", 1)[1] for p in (s.bronze_dir / dataset).glob("release_date=*")
              if p.is_dir() and not p.name.endswith(".tmp")}
    if full_refresh:
        return sorted(bronze)
    part_root = s.silver_dir / silver_table
    if silver_table == "measure_scores":
        part_root = part_root / f"source_dataset={dataset}"
    done = {p.name.split("=", 1)[1] for p in part_root.glob("release_date=*")}
    return sorted(bronze - done)


def read_bronze(spark: SparkSession, dataset: str, releases: list[str]) -> DataFrame:
    s = get_settings()
    paths = [str(s.bronze_dir / dataset / f"release_date={r}") for r in releases]
    return (spark.read.option("basePath", str(s.bronze_dir / dataset))
            .option("mergeSchema", "true").parquet(*paths)
            .withColumn("release_date", F.col("release_date").cast("string"))
            .withColumn("_ingested_at", F.to_timestamp("_ingested_at")))


def _base_rules() -> dict[str, F.Column]:
    return {
        "invalid_facility_id": F.col("facility_id").rlike(CCN_PATTERN),
        "invalid_state": F.col("state").isin(VALID_STATES),
    }


# --------------------------------------------------------------------------- per-table logic
def transform_hospitals(df: DataFrame) -> tuple[DataFrame, DataFrame]:
    df = nullify_sentinels(df)
    good, bad = split_valid(df, _base_rules() | {"missing_name": F.col("facility_name").isNotNull()})
    counts = {}
    for grp in ("mort", "safety", "readm"):
        for kind in ("better", "no_different", "worse"):
            counts[f"{grp}_measures_{kind}"] = try_int(f"count_of_{grp}_measures_{kind}")
    out = good.select(
        "facility_id",
        F.initcap("facility_name").alias("facility_name"),
        F.initcap("address").alias("address"),
        F.initcap("citytown").alias("city"),
        "state",
        F.lpad(F.regexp_extract("zip_code", r"(\d+)", 1), 5, "0").alias("zip_code"),
        F.initcap("countyparish").alias("county"),
        F.col("telephone_number").alias("phone"),
        "hospital_type",
        F.col("hospital_ownership").alias("ownership"),
        yes_no("emergency_services").alias("has_emergency_services"),
        F.coalesce(yes_no("meets_criteria_for_birthing_friendly_designation"), F.lit(False))
         .alias("is_birthing_friendly"),
        try_int("hospital_overall_rating").alias("overall_rating"),
        F.col("hospital_overall_rating_footnote").alias("overall_rating_footnote"),
        *[c.alias(n) for n, c in counts.items()],
        "release_date",
        "_ingested_at",
    )
    return out, bad


def transform_measures(df: DataFrame, dataset: str) -> tuple[DataFrame, DataFrame, int]:
    df = nullify_sentinels(df)
    for c in ("number_of_patients", "number_of_patients_returned"):
        if c not in df.columns:
            df = df.withColumn(c, F.lit(None).cast("string"))
    good, bad = split_valid(df, _base_rules() | {"missing_measure_id": F.col("measure_id").isNotNull()})
    out = good.select(
        "facility_id",
        "measure_id",
        "measure_name",
        "compared_to_national",
        try_double("denominator").alias("denominator"),
        try_double("score").alias("score"),
        try_double("lower_estimate").alias("lower_estimate"),
        try_double("higher_estimate").alias("higher_estimate"),
        try_int("number_of_patients").alias("number_of_patients"),
        try_int("number_of_patients_returned").alias("number_of_patients_returned"),
        "footnote",
        try_date("start_date").alias("period_start"),
        try_date("end_date").alias("period_end"),
        F.lit(dataset).alias("source_dataset"),
        "release_date",
        "_ingested_at",
        # raw value kept to count parse failures below
        F.col("score").alias("_raw_score"),
    )
    unparseable = out.filter(F.col("_raw_score").isNotNull() & F.col("score").isNull()).count()
    return out.drop("_raw_score"), bad, unparseable


def transform_hcahps(df: DataFrame) -> tuple[DataFrame, DataFrame]:
    df = nullify_sentinels(df)
    good, bad = split_valid(df, _base_rules() | {"missing_measure_id": F.col("hcahps_measure_id").isNotNull()})
    out = good.select(
        "facility_id",
        F.col("hcahps_measure_id").alias("measure_id"),
        F.col("hcahps_question").alias("question"),
        F.col("hcahps_answer_description").alias("answer_description"),
        try_int("patient_survey_star_rating").alias("star_rating"),
        try_int("hcahps_answer_percent").alias("answer_percent"),
        try_int("hcahps_linear_mean_value").alias("linear_mean_value"),
        try_int("number_of_completed_surveys").alias("completed_surveys"),
        try_int("survey_response_rate_percent").alias("response_rate_pct"),
        try_date("start_date").alias("period_start"),
        try_date("end_date").alias("period_end"),
        "release_date",
        "_ingested_at",
    )
    return out, bad


# --------------------------------------------------------------------------- writers
def _write(df: DataFrame, path: Path, partition_cols: list[str]) -> None:
    (df.repartition(*partition_cols).write.mode("overwrite")
       .partitionBy(*partition_cols).parquet(str(path)))


def _quarantine(bad: DataFrame, table: str) -> int:
    n = bad.count()
    if n:
        _write(bad, get_settings().quarantine_dir / table, ["release_date"])
    return n


def run(spark: SparkSession | None = None, full_refresh: bool = False) -> list[dict]:
    """Process all pending bronze releases into silver. Returns per-table stats."""
    s = get_settings()
    spark = spark or get_spark()
    stats: list[TableStats] = []

    for table, datasets in SILVER_SOURCES.items():
        for dataset in datasets:
            releases = pending_releases(dataset, table, full_refresh)
            st = TableStats(table=f"{table}<-{dataset}", releases=releases)
            if not releases:
                log.info("silver %s <- %s: nothing new", table, dataset)
                stats.append(st)
                continue
            raw = read_bronze(spark, dataset, releases).cache()
            st.rows_in = raw.count()

            if table == "hospitals":
                good, bad = transform_hospitals(raw)
                keys, parts = ["facility_id", "release_date"], ["release_date"]
            elif table == "measure_scores":
                good, bad, st.unparseable_numerics = transform_measures(raw, dataset)
                keys, parts = ["facility_id", "measure_id", "release_date"], ["source_dataset", "release_date"]
            else:
                good, bad = transform_hcahps(raw)
                keys, parts = ["facility_id", "measure_id", "release_date"], ["release_date"]

            good = good.cache()
            before = good.count()
            deduped = dedupe(good, keys)
            st.rows_out = deduped.count()
            st.duplicates_removed = before - st.rows_out
            st.quarantined = _quarantine(bad, dataset)
            _write(deduped, s.silver_dir / table, parts)
            raw.unpersist()
            good.unpersist()
            log.info("silver %s", st)
            stats.append(st)
    return [asdict(x) for x in stats]
