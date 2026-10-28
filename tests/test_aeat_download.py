import io
import zipfile

from es_energy_trade.ingestion.aeat_discovery import ZipLink
from es_energy_trade.ingestion.aeat_download import download_and_unpack

LINK = ZipLink(url="https://example.test/2025/enero/cg25en39.zip", year=2025, month=1,
               month_code="en", last_chapter="39", file_name="cg25en39.zip")


def make_zip(content: bytes) -> bytes:
    buf = io.BytesIO()
    with zipfile.ZipFile(buf, "w") as zf:
        zf.writestr("cg25en39", content)
    return buf.getvalue()


class FakeSession:
    def __init__(self, payload):
        self.payload, self.calls = payload, 0

    def get(self, url, **kwargs):
        self.calls += 1
        payload = self.payload

        class Resp:
            def __enter__(self): return self
            def __exit__(self, *a): return False
            def raise_for_status(self): pass
            def iter_content(self, n): yield payload
        return Resp()


def test_download_unpacks_next_to_the_zip_and_is_idempotent(tmp_path):
    session = FakeSession(make_zip(b"line1\r\nline2\r\n"))
    path = download_and_unpack(LINK, str(tmp_path), session)
    assert path == tmp_path / "aeat" / "legacy" / "2025-01" / "cg25en39"
    assert path.read_bytes() == b"line1\r\nline2\r\n"
    assert (path.parent / "cg25en39.zip").exists()  # the ZIP is kept as delivered

    download_and_unpack(LINK, str(tmp_path), session)
    assert session.calls == 1  # second run skips the month


import json

import pytest
import requests

from es_energy_trade.ingestion.aeat_discovery import CsvLink
from es_energy_trade.ingestion.aeat_download import (
    CSV_NAME, download_csv_snapshot, ensure_manifest, latest_csv_snapshot, read_manifest,
)

JULY = CsvLink(url="https://example.test/Nuevo_formato_2026_max_des/julio/MAXIMA_DESAGREGACION_2026.csv", through_month=7)
AUGUST = CsvLink(url="https://example.test/Nuevo_formato_2026_max_des/agosto/MAXIMA_DESAG_AGOSTO26.csv", through_month=8)


class CsvSession:
    """Serves ``payload``; optionally fails the first ``fail_first`` calls like a flaky connection."""

    def __init__(self, payload, headers=None, length=None, fail_first=0):
        self.payload, self.extra, self.length, self.fail_first, self.calls = payload, headers or {}, length, fail_first, 0

    def get(self, url, **kwargs):
        self.calls += 1
        if self.calls <= self.fail_first:
            raise requests.ConnectionError("connection reset")
        payload = self.payload
        headers = {"Content-Length": str(len(payload) if self.length is None else self.length), **self.extra}

        class Resp:
            def __enter__(self): return self
            def __exit__(self, *a): return False
            def raise_for_status(self): pass
            def iter_content(self, n): yield payload
        Resp.headers = headers
        return Resp()


def snapshot_names(root):
    return sorted(p.name for p in (root / "aeat" / "csv_2026").iterdir())


def test_csv_snapshot_is_downloaded_with_a_manifest(tmp_path):
    s = CsvSession(b"I;2026;01;27090090;DE;48;1;0;1.00;;3;UE;11;48\n", {"ETag": '"v1"'})
    path, downloaded = download_csv_snapshot(AUGUST, str(tmp_path), "2026-10-28", s)
    assert downloaded and path == tmp_path / "aeat" / "csv_2026" / "snapshot=2026-10-28" / CSV_NAME
    m = read_manifest(path)
    assert (m["url"], m["through_month"], m["origin"], m["etag"]) == (AUGUST.url, 8, "download", '"v1"')
    assert len(m["sha256"]) == 64 and m["size_bytes"] == path.stat().st_size


def test_same_release_proven_by_validators_is_not_downloaded_again(tmp_path):
    s = CsvSession(b"aaa\n", {"ETag": '"v1"'})
    download_csv_snapshot(AUGUST, str(tmp_path), "2026-10-28", s)
    path, downloaded = download_csv_snapshot(AUGUST, str(tmp_path), "2026-11-05", s)
    assert not downloaded and snapshot_names(tmp_path) == ["snapshot=2026-10-28"]


def test_same_size_but_different_content_is_a_new_snapshot(tmp_path):
    # no ETag / Last-Modified: size alone must not be trusted
    download_csv_snapshot(AUGUST, str(tmp_path), "2026-10-28", CsvSession(b"aaa\n"))
    path, downloaded = download_csv_snapshot(AUGUST, str(tmp_path), "2026-11-05", CsvSession(b"bbb\n"))
    assert downloaded and path.parent.name == "snapshot=2026-11-05"
    assert snapshot_names(tmp_path) == ["snapshot=2026-10-28", "snapshot=2026-11-05"]


def test_identical_bytes_under_a_new_url_are_not_a_new_snapshot(tmp_path):
    download_csv_snapshot(JULY, str(tmp_path), "2026-10-16", CsvSession(b"same\n"))
    path, downloaded = download_csv_snapshot(AUGUST, str(tmp_path), "2026-10-28", CsvSession(b"same\n"))
    assert not downloaded and snapshot_names(tmp_path) == ["snapshot=2026-10-16"]


def test_two_downloads_on_the_same_day_never_overwrite_each_other(tmp_path):
    download_csv_snapshot(JULY, str(tmp_path), "2026-10-28", CsvSession(b"one\n"))
    path, _ = download_csv_snapshot(AUGUST, str(tmp_path), "2026-10-28", CsvSession(b"two\n"))
    assert snapshot_names(tmp_path) == ["snapshot=2026-10-28", "snapshot=2026-10-28-2"]
    assert latest_csv_snapshot(str(tmp_path)) == path


def test_transient_failures_are_retried(tmp_path):
    s = CsvSession(b"ok\n", fail_first=2)
    path, downloaded = download_csv_snapshot(AUGUST, str(tmp_path), "2026-10-28", s, retries=3, backoff_s=0)
    assert downloaded and s.calls == 3


def test_truncated_download_leaves_nothing_and_fails_after_retries(tmp_path):
    with pytest.raises(IOError, match="failed after 2 attempts"):
        download_csv_snapshot(AUGUST, str(tmp_path), "2026-10-28", CsvSession(b"abc", length=10), retries=2, backoff_s=0)
    assert not list(tmp_path.rglob("*.csv")) and not list(tmp_path.rglob("snapshot=*"))


def test_a_snapshot_put_there_by_hand_gets_a_manifest_from_its_content(tmp_path):
    folder = tmp_path / "aeat" / "csv_2026" / "snapshot=2026-10-16"
    folder.mkdir(parents=True)
    f = folder / CSV_NAME
    f.write_bytes(b"manual\n")
    m = ensure_manifest(f)
    assert m["origin"] == "manual_upload" and m["through_month"] is None
    assert json.loads((folder / "manifest.json").read_text())["sha256"] == m["sha256"]


def test_passing_the_url_instead_of_the_link_fails_with_a_clear_message(tmp_path):
    with pytest.raises(TypeError, match="CsvLink"):
        download_csv_snapshot(AUGUST.url, str(tmp_path), "2026-10-28", CsvSession(b"x"))
