"""Central configuration: paths, dataset registry, environment overrides.

Everything is driven by two environment variables so the same code runs on a
laptop, in Docker, in Airflow and in CI:

* ``DATA_DIR``     - root of the lakehouse (bronze / silver / gold / warehouse)
* ``PIPELINE_SOURCE`` - ``api`` (live CMS) or ``sample`` (offline synthetic data)
"""

from __future__ import annotations

import os
from dataclasses import dataclass, field
from functools import lru_cache
from pathlib import Path

import yaml

# Repo root (holds config/ and dbt/). Overridable for installed (non-editable) deployments.
PROJECT_ROOT = Path(os.getenv("PROJECT_ROOT", Path(__file__).resolve().parents[2]))
CONFIG_PATH = PROJECT_ROOT / "config" / "datasets.yml"


@dataclass(frozen=True)
class DatasetConfig:
    name: str
    dataset_id: str
    title: str
    primary_key: list[str]
    required_columns: list[str]


@dataclass(frozen=True)
class Settings:
    api_base: str
    page_size: int
    request_timeout_s: int
    datasets: dict[str, DatasetConfig]
    data_dir: Path = field(default_factory=lambda: Path(os.getenv("DATA_DIR", PROJECT_ROOT / "data")))

    # ---- lakehouse layout -------------------------------------------------
    @property
    def bronze_dir(self) -> Path:
        return self.data_dir / "bronze"

    @property
    def silver_dir(self) -> Path:
        return self.data_dir / "silver"

    @property
    def quarantine_dir(self) -> Path:
        return self.data_dir / "quarantine"

    @property
    def gold_dir(self) -> Path:
        return self.data_dir / "gold"

    @property
    def warehouse_path(self) -> Path:
        return self.data_dir / "warehouse" / "hospital_quality.duckdb"

    def ensure_dirs(self) -> None:
        for p in (self.bronze_dir, self.silver_dir, self.quarantine_dir, self.gold_dir,
                  self.warehouse_path.parent):
            p.mkdir(parents=True, exist_ok=True)


@lru_cache(maxsize=1)
def get_settings() -> Settings:
    raw = yaml.safe_load(CONFIG_PATH.read_text())
    datasets = {
        name: DatasetConfig(
            name=name,
            dataset_id=d["dataset_id"],
            title=d["title"],
            primary_key=d["primary_key"],
            required_columns=d["required_columns"],
        )
        for name, d in raw["datasets"].items()
    }
    return Settings(
        api_base=os.getenv("CMS_API_BASE", raw["api_base"]),
        page_size=int(raw["page_size"]),
        request_timeout_s=int(raw["request_timeout_s"]),
        datasets=datasets,
    )
