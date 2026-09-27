import os

import pytest

from hospital_pipeline import config


@pytest.fixture()
def data_dir(tmp_path, monkeypatch):
    """Point the whole pipeline at an isolated, empty lakehouse."""
    monkeypatch.setenv("DATA_DIR", str(tmp_path / "data"))
    config.get_settings.cache_clear()
    s = config.get_settings()
    s.ensure_dirs()
    yield s.data_dir
    config.get_settings.cache_clear()


@pytest.fixture(scope="session")
def spark():
    from hospital_pipeline.transform.spark_session import get_spark

    os.environ.setdefault("SPARK_SHUFFLE_PARTITIONS", "2")
    session = get_spark("tests")
    session.sparkContext.setLogLevel("ERROR")
    yield session
    session.stop()
