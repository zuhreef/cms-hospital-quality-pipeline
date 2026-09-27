import requests
import responses

from hospital_pipeline.ingest.cms_client import CMSClient

BASE = "https://example.test/api/1"


def _page(rows, count):
    return {"count": count, "results": rows}


@responses.activate
def test_paginates_until_count_exhausted():
    url = f"{BASE}/datastore/query/abc/0"
    responses.get(url, json=_page([{"id": i} for i in range(2)], 5))
    responses.get(url, json=_page([{"id": i} for i in range(2, 4)], 5))
    responses.get(url, json=_page([{"id": 4}], 5))

    rows = CMSClient(BASE, page_size=2).fetch_all("abc")

    assert [r["id"] for r in rows] == [0, 1, 2, 3, 4]
    offsets = [c.request.params["offset"] for c in responses.calls]
    assert offsets == ["0", "2", "4"]


@responses.activate
def test_stops_on_empty_page_even_if_count_is_wrong():
    url = f"{BASE}/datastore/query/abc/0"
    responses.get(url, json=_page([{"id": 1}], 100))
    responses.get(url, json=_page([], 100))
    assert len(CMSClient(BASE, page_size=1).fetch_all("abc")) == 1


@responses.activate
def test_retries_transient_errors(monkeypatch):
    monkeypatch.setattr("time.sleep", lambda *_: None)  # skip backoff waits
    url = f"{BASE}/datastore/query/abc/0"
    responses.get(url, status=503)
    responses.get(url, status=429)
    responses.get(url, json=_page([{"id": 1}], 1))
    assert CMSClient(BASE).fetch_all("abc") == [{"id": 1}]
    assert len(responses.calls) == 3


@responses.activate
def test_does_not_retry_client_errors():
    responses.get(f"{BASE}/datastore/query/missing/0", status=404)
    try:
        CMSClient(BASE).fetch_all("missing")
        raise AssertionError("expected HTTPError")
    except requests.HTTPError:
        pass
    assert len(responses.calls) == 1


@responses.activate
def test_metadata_parses_release_date():
    responses.get(f"{BASE}/metastore/schemas/dataset/items/xubh-q36u", json={
        "title": "Hospital General Information", "modified": "2026-07-22",
        "distribution": [{"downloadURL": "https://example.test/file.csv"}]})
    meta = CMSClient(BASE).get_metadata("xubh-q36u")
    assert meta.modified == "2026-07-22"
    assert meta.download_url.endswith("file.csv")
