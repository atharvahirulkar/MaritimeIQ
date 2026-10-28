"""Spark + Delta Lake session factory for local[*] mode."""
from __future__ import annotations

import os
import sys
from functools import lru_cache
from typing import Any

import yaml
from delta import configure_spark_with_delta_pip
from pyspark.sql import SparkSession

from maritimeiq.utils.paths import LAKEHOUSE, SPARK_CFG, SRC, ensure_lakehouse_dirs


def _load_cfg() -> dict[str, Any]:
    with SPARK_CFG.open(encoding="utf-8") as f:
        return yaml.safe_load(f)


@lru_cache(maxsize=1)
def get_spark(app_name: str | None = None) -> SparkSession:
    """Create or reuse a local Spark session with Delta support."""
    ensure_lakehouse_dirs()
    cfg = _load_cfg()
    warehouse = str(LAKEHOUSE.resolve())
    # Workers must run the driver's interpreter (minor versions must match) and be able to
    # import maritimeiq for UDFs even without `pip install -e .`
    py = os.environ.get("MARITIMEIQ_PYTHON") or sys.executable
    os.environ["PYSPARK_PYTHON"] = py
    os.environ["PYSPARK_DRIVER_PYTHON"] = py
    src = str(SRC)
    parts = [p for p in os.environ.get("PYTHONPATH", "").split(os.pathsep) if p]
    if src not in parts:
        os.environ["PYTHONPATH"] = os.pathsep.join([src, *parts])

    builder = (
        SparkSession.builder.appName(app_name or cfg.get("app_name", "MaritimeIQ"))
        .master(cfg.get("master", "local[*]"))
        .config("spark.driver.memory", cfg.get("driver_memory", "4g"))
        .config("spark.sql.shuffle.partitions", str(cfg.get("shuffle_partitions", 8)))
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
        .config("spark.sql.warehouse.dir", warehouse)
        .config("spark.databricks.delta.retentionDurationCheck.enabled", "false")
        .config("spark.ui.enabled", "false")
        .config("spark.driver.host", "127.0.0.1")
        .config("spark.sql.session.timeZone", "UTC")
    )
    spark = configure_spark_with_delta_pip(builder).getOrCreate()
    spark.sparkContext.setLogLevel("WARN")
    return spark


def stop_spark() -> None:
    try:
        spark = SparkSession.getActiveSession()
        if spark is not None:
            spark.stop()
    finally:
        get_spark.cache_clear()
