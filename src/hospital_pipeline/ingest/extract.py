"""Extract step: source -> bronze (raw, immutable, partitioned by CMS release).

Bronze keeps every source column as a string exactly as delivered, plus audit
columns. Nothing is cleaned here - that is silver's job - so bronze can always
be replayed if transformation logic changes.

Layout::

    data/bronze/<dataset>/release_date=YYYY-MM-DD/part-0.parquet
    data/bronze/<dataset>/release_date=YYYY-MM-DD/_manifest.json

Idempotency / change detection: a release already recorded in
``ops.ingest_manifest`` is skipped unless ``force=True``.
"""

from __future__ import annotations

import hashlib
import json
import logging
import shutil
from dataclasses import dataclass
from pathlib import Path

import pyarrow as pa
import pyarrow.parquet as pq

from hospital_pipeline import ops
from hospital_pipeline.config import DatasetConfig, get_settings
from hospital_pipeline.ingest import sample_data
from hospital_pipeline.ingest.cms_client import CMSClient

log = logging.getLogger(__name__)


@dataclass
class ExtractResult:
    dataset: str
    release_date: str
    rows: int
    skipped: bool
    path: str | None = None


def _to_table(rows: list[dict], run_id: str, release: str, source: str) -> pa.Table:
    columns = sorted({k for r in rows for k in r})
    data = {c: [None if r.get(c) is None else str(r.get(c)) for r in rows] for c in columns}
    ingested_at = ops.now().isoformat()
    n = len(rows)
    data |= {"_run_id": [run_id] * n, "_source": [source] * n,
             "_ingested_at": [ingested_at] * n, "_release_date": [release] * n}
    return pa.table({k: pa.array(v, type=pa.string()) for k, v in data.items()})


def _content_hash(rows: list[dict]) -> str:
    h = hashlib.sha256()
    for r in rows:
        h.update(json.dumps(r, sort_keys=True).encode())
    return h.hexdigest()


def write_bronze(ds: DatasetConfig, rows: list[dict], release: str, run_id: str, source: str) -> Path:
    """Atomically write one release partition (tmp dir -> rename)."""
    s = get_settings()
    final_dir = s.bronze_dir / ds.name / f"release_date={release}"
    tmp_dir = final_dir.with_name(final_dir.name + ".tmp")
    shutil.rmtree(tmp_dir, ignore_errors=True)
    tmp_dir.mkdir(parents=True)
    pq.write_table(_to_table(rows, run_id, release, source), tmp_dir / "part-0.parquet",
                   compression="zstd")
    (tmp_dir / "_manifest.json").write_text(json.dumps({
        "dataset": ds.name, "dataset_id": ds.dataset_id, "release_date": release,
        "rows": len(rows), "content_sha256": _content_hash(rows), "run_id": run_id, "source": source,
    }, indent=2))
    shutil.rmtree(final_dir, ignore_errors=True)
    tmp_dir.rename(final_dir)
    return final_dir


def _land(ds: DatasetConfig, rows: list[dict], release: str, run_id: str, source: str) -> ExtractResult:
    if not rows:
        raise ValueError(f"{ds.name}: source returned 0 rows for release {release}")
    path = write_bronze(ds, rows, release, run_id, source)
    ops.record_ingest(run_id, ds.name, ds.dataset_id, release, len(rows), _content_hash(rows), str(path))
    log.info("bronze %s release=%s rows=%s", ds.name, release, len(rows))
    return ExtractResult(ds.name, release, len(rows), skipped=False, path=str(path))


def extract_dataset(name: str, run_id: str, source: str = "api", force: bool = False) -> list[ExtractResult]:
    s = get_settings()
    ds = s.datasets[name]
    already = ops.ingested_releases(name)

    if source == "sample":
        results = []
        for release in sample_data.SAMPLE_RELEASES:          # backfill every release not yet landed
            if release in already and not force:
                results.append(ExtractResult(name, release, 0, skipped=True))
                continue
            rows = sample_data.generate_release(release)[name]
            results.append(_land(ds, rows, release, run_id, source))
        return results

    client = CMSClient(s.api_base, s.page_size, s.request_timeout_s)
    meta = client.get_metadata(ds.dataset_id)
    release = meta.modified or ops.now().date().isoformat()
    if release in already and not force:
        log.info("%s release %s already ingested - skipping", name, release)
        return [ExtractResult(name, release, 0, skipped=True)]
    return [_land(ds, client.fetch_all(ds.dataset_id), release, run_id, source)]
