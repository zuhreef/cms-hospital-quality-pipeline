"""Thin, resilient client for the CMS Provider Data Catalog (DKAN) API.

Features that matter in production extractors:
* offset pagination until the reported ``count`` is exhausted
* retries with exponential backoff on transient errors (429 / 5xx / timeouts)
* a metadata call used for change detection (``modified`` date of the release)
"""

from __future__ import annotations

import logging
from collections.abc import Iterator
from dataclasses import dataclass

import requests
from tenacity import (
    retry,
    retry_if_exception,
    stop_after_attempt,
    wait_exponential,
)

log = logging.getLogger(__name__)

RETRYABLE_STATUS = {429, 500, 502, 503, 504}


def _is_retryable(exc: BaseException) -> bool:
    if isinstance(exc, (requests.ConnectionError, requests.Timeout)):
        return True
    if isinstance(exc, requests.HTTPError) and exc.response is not None:
        return exc.response.status_code in RETRYABLE_STATUS
    return False


@dataclass(frozen=True)
class DatasetMetadata:
    dataset_id: str
    title: str
    modified: str  # ISO date of the CMS release, e.g. "2026-07-22"
    download_url: str | None


class CMSClient:
    def __init__(self, api_base: str, page_size: int = 1500, timeout_s: int = 60,
                 session: requests.Session | None = None) -> None:
        self.api_base = api_base.rstrip("/")
        self.page_size = page_size
        self.timeout_s = timeout_s
        self.session = session or requests.Session()
        self.session.headers.update({"User-Agent": "cms-hospital-quality-pipeline/0.1"})

    @retry(
        retry=retry_if_exception(_is_retryable),
        wait=wait_exponential(multiplier=1, min=1, max=30),
        stop=stop_after_attempt(5),
        reraise=True,
    )
    def _get(self, path: str, params: dict | None = None) -> dict:
        resp = self.session.get(f"{self.api_base}{path}", params=params, timeout=self.timeout_s)
        resp.raise_for_status()
        return resp.json()

    def get_metadata(self, dataset_id: str) -> DatasetMetadata:
        body = self._get(f"/metastore/schemas/dataset/items/{dataset_id}")
        dists = body.get("distribution") or []
        return DatasetMetadata(
            dataset_id=dataset_id,
            title=body.get("title", dataset_id),
            modified=str(body.get("modified", ""))[:10],
            download_url=dists[0].get("downloadURL") if dists else None,
        )

    def iter_pages(self, dataset_id: str) -> Iterator[list[dict]]:
        """Yield pages of records until the dataset is exhausted."""
        offset, total = 0, None
        while total is None or offset < total:
            body = self._get(
                f"/datastore/query/{dataset_id}/0",
                params={"limit": self.page_size, "offset": offset, "count": "true",
                        "results": "true", "schema": "false", "keys": "true"},
            )
            total = int(body.get("count", 0))
            rows = body.get("results", [])
            if not rows:
                break
            log.info("dataset=%s offset=%s fetched=%s total=%s", dataset_id, offset, len(rows), total)
            yield rows
            offset += len(rows)

    def fetch_all(self, dataset_id: str) -> list[dict]:
        return [row for page in self.iter_pages(dataset_id) for row in page]
