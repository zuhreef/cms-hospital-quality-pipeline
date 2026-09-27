"""Operational metadata: run log, task log, ingestion manifest and DQ results.

Stored in the ``ops`` schema of the DuckDB warehouse so lineage/observability
data lives next to the data it describes and can be surfaced on the
dashboard's Pipeline Health page.
"""

from __future__ import annotations

import json
import time
import uuid
from collections.abc import Iterator
from contextlib import contextmanager
from datetime import UTC, datetime

import duckdb

from hospital_pipeline.config import get_settings

DDL = """
create schema if not exists ops;

create table if not exists ops.pipeline_runs (
    run_id        varchar primary key,
    source        varchar,
    trigger       varchar,
    started_at    timestamp,
    finished_at   timestamp,
    status        varchar,          -- running | success | failed | skipped
    error         varchar
);

create table if not exists ops.task_runs (
    run_id        varchar,
    task          varchar,
    started_at    timestamp,
    finished_at   timestamp,
    duration_s    double,
    status        varchar,
    rows_out      bigint,
    details       varchar           -- JSON blob
);

create table if not exists ops.ingest_manifest (
    run_id          varchar,
    dataset         varchar,
    dataset_id      varchar,
    release_date    varchar,
    rows            bigint,
    content_sha256  varchar,
    bronze_path     varchar,
    ingested_at     timestamp,
    primary key (dataset, release_date)
);

create table if not exists ops.dq_results (
    run_id        varchar,
    checked_at    timestamp,
    layer         varchar,
    dataset       varchar,
    check_name    varchar,
    severity      varchar,          -- error | warn
    passed        boolean,
    observed      varchar,
    threshold     varchar,
    message       varchar
);
"""


def now() -> datetime:
    return datetime.now(UTC).replace(tzinfo=None)


def connect(read_only: bool = False) -> duckdb.DuckDBPyConnection:
    s = get_settings()
    s.ensure_dirs()
    con = duckdb.connect(str(s.warehouse_path), read_only=read_only)
    if not read_only:
        con.execute(DDL)
    return con


def new_run_id() -> str:
    return datetime.now(UTC).strftime("%Y%m%dT%H%M%S") + "-" + uuid.uuid4().hex[:6]


def start_run(run_id: str, source: str, trigger: str) -> None:
    with connect() as con:
        con.execute("insert into ops.pipeline_runs values (?, ?, ?, ?, null, 'running', null)",
                    [run_id, source, trigger, now()])


def finish_run(run_id: str, status: str, error: str | None = None) -> None:
    with connect() as con:
        con.execute("update ops.pipeline_runs set finished_at = ?, status = ?, error = ? where run_id = ?",
                    [now(), status, error, run_id])


@contextmanager
def track_task(run_id: str, task: str) -> Iterator[dict]:
    """Context manager that times a task and records it in ops.task_runs.

    The body can set ``info["rows_out"]`` and add any JSON-serialisable
    details to ``info`` which are persisted with the task record.
    """
    info: dict = {}
    started, t0 = now(), time.perf_counter()
    status = "success"
    try:
        yield info
    except Exception as exc:
        status = "failed"
        info["error"] = repr(exc)[:500]
        raise
    finally:
        rows = info.pop("rows_out", None)
        with connect() as con:
            con.execute("insert into ops.task_runs values (?, ?, ?, ?, ?, ?, ?, ?)",
                        [run_id, task, started, now(), round(time.perf_counter() - t0, 3), status,
                         rows, json.dumps(info, default=str)])


def ingested_releases(dataset: str) -> set[str]:
    with connect() as con:
        rows = con.execute("select release_date from ops.ingest_manifest where dataset = ?",
                           [dataset]).fetchall()
    return {r[0] for r in rows}


def record_ingest(run_id: str, dataset: str, dataset_id: str, release: str, rows: int,
                  sha: str, path: str) -> None:
    with connect() as con:
        con.execute("delete from ops.ingest_manifest where dataset = ? and release_date = ?",
                    [dataset, release])
        con.execute("insert into ops.ingest_manifest values (?, ?, ?, ?, ?, ?, ?, ?)",
                    [run_id, dataset, dataset_id, release, rows, sha, path, now()])


def record_dq(run_id: str, results: list[dict]) -> None:
    if not results:
        return
    with connect() as con:
        con.executemany(
            "insert into ops.dq_results values (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
            [[run_id, now(), r["layer"], r["dataset"], r["check_name"], r["severity"], r["passed"],
              str(r.get("observed")), str(r.get("threshold")), r.get("message", "")] for r in results],
        )
