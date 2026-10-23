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


class CsvSession:
    def __init__(self, payload, length=None):
        self.payload, self.length, self.calls = payload, length, 0

    def get(self, url, **kwargs):
        self.calls += 1
        payload, length = self.payload, len(self.payload) if self.length is None else self.length

        class Resp:
            headers = {"Content-Length": str(length)}
            def __enter__(self): return self
            def __exit__(self, *a): return False
            def raise_for_status(self): pass
            def iter_content(self, n): yield payload
        return Resp()


def test_csv_snapshot_downloads_then_skips_when_size_is_unchanged(tmp_path):
    from es_energy_trade.ingestion.aeat_download import download_csv_snapshot
    s = CsvSession(b"I;2026;01;27090090;DE;48;1;0;1.00;;3;UE;11;48\n")
    path, downloaded = download_csv_snapshot("u", str(tmp_path), "2026-10-20", s)
    assert downloaded and path == tmp_path / "aeat" / "csv_2026" / "snapshot=2026-10-20" / "MAXIMA_DESAGREGACION_2026.csv"

    path2, downloaded2 = download_csv_snapshot("u", str(tmp_path), "2026-10-27", s)
    assert not downloaded2 and path2 == path
    assert not (tmp_path / "aeat" / "csv_2026" / "snapshot=2026-10-27").exists()


def test_csv_snapshot_keeps_the_previous_snapshot_when_the_file_changes(tmp_path):
    from es_energy_trade.ingestion.aeat_download import download_csv_snapshot
    download_csv_snapshot("u", str(tmp_path), "2026-10-20", CsvSession(b"aaa\n"))
    path, downloaded = download_csv_snapshot("u", str(tmp_path), "2026-11-20", CsvSession(b"aaa\nbbb\n"))
    assert downloaded and path.parent.name == "snapshot=2026-11-20"
    assert (tmp_path / "aeat" / "csv_2026" / "snapshot=2026-10-20" / "MAXIMA_DESAGREGACION_2026.csv").exists()


def test_csv_truncated_download_fails_and_leaves_no_file(tmp_path):
    import pytest
    from es_energy_trade.ingestion.aeat_download import download_csv_snapshot
    with pytest.raises(IOError, match="expected"):
        download_csv_snapshot("u", str(tmp_path), "2026-10-20", CsvSession(b"abc", length=10))
    assert not list(tmp_path.rglob("*.csv"))
