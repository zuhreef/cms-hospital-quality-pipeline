from __future__ import annotations

import os

from pyspark.sql import SparkSession


def get_spark(app_name: str = "cms-hospital-quality") -> SparkSession:
    """Local SparkSession tuned for a laptop / CI runner.

    On a cluster the same job runs unchanged via ``spark-submit --master yarn|k8s``;
    only SPARK_MASTER changes.
    """
    return (
        SparkSession.builder.appName(app_name)
        .master(os.getenv("SPARK_MASTER", "local[*]"))
        .config("spark.sql.shuffle.partitions", os.getenv("SPARK_SHUFFLE_PARTITIONS", "8"))
        .config("spark.sql.sources.partitionOverwriteMode", "dynamic")
        .config("spark.sql.session.timeZone", "UTC")
        .config("spark.sql.parquet.compression.codec", "zstd")
        .config("spark.ui.enabled", "false")
        .config("spark.driver.memory", os.getenv("SPARK_DRIVER_MEMORY", "2g"))
        .getOrCreate()
    )
