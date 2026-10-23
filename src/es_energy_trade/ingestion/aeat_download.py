"""Download AEAT ZIPs into a Unity Catalog volume and unpack the data file (Bronze, legacy format).

The ZIP is kept exactly as delivered; the unpacked fixed-width file sits next to it so Spark can read it.
Idempotent: a month already unpacked is skipped.
"""
from __future__ import annotations

import pathlib
import zipfile

import requests

from es_energy_trade.ingestion.aeat_discovery import ZipLink

CHUNK = 1024 * 1024


def month_dir(volume_root: str, link: ZipLink) -> pathlib.Path:
    return pathlib.Path(volume_root) / "aeat" / "legacy" / f"{link.year}-{link.month:02d}"


def download_and_unpack(link: ZipLink, volume_root: str, session: requests.Session | None = None) -> pathlib.Path:
    """Return the path of the unpacked data file (e.g. ``.../2025-01/cg25en39``)."""
    target = month_dir(volume_root, link)
    target.mkdir(parents=True, exist_ok=True)
    data_file = target / link.file_name.removesuffix(".zip")
    if data_file.exists() and data_file.stat().st_size > 0:
        return data_file

    zip_path = target / link.file_name
    http = session or requests.Session()
    with http.get(link.url, stream=True, timeout=(10, 120)) as resp:
        resp.raise_for_status()
        with open(zip_path, "wb") as fh:
            for chunk in resp.iter_content(CHUNK):
                fh.write(chunk)

    with zipfile.ZipFile(zip_path) as zf:
        members = [m for m in zf.namelist() if not m.endswith("/")]
        if len(members) != 1:
            raise ValueError(f"{zip_path.name}: expected 1 file inside the ZIP, found {members}")
        with zf.open(members[0]) as src, open(data_file, "wb") as dst:
            while chunk := src.read(CHUNK):
                dst.write(chunk)
    return data_file


# --- shared HTTP helpers --------------------------------------------------------------------------

USER_AGENT = "es-energy-trade-lakehouse/0.1 (portfolio project; contact via GitHub)"


def http_fetch(url: str, session: requests.Session | None = None, pause_s: float = 1.0) -> str:
    """GET a page as text. Short pause between calls: be polite to a public site."""
    import time

    http = session or requests.Session()
    resp = http.get(url, headers={"User-Agent": USER_AGENT}, timeout=(10, 60))
    resp.raise_for_status()
    resp.encoding = resp.encoding or "utf-8"
    time.sleep(pause_s)
    return resp.text


# --- 2026 cumulative CSV --------------------------------------------------------------------------

CSV_NAME = "MAXIMA_DESAGREGACION_2026.csv"


def csv_snapshots_dir(volume_root: str) -> pathlib.Path:
    return pathlib.Path(volume_root) / "aeat" / "csv_2026"


def latest_csv_snapshot(volume_root: str) -> pathlib.Path | None:
    snaps = sorted(csv_snapshots_dir(volume_root).glob(f"snapshot=*/{CSV_NAME}"))
    return snaps[-1] if snaps else None


def download_csv_snapshot(url: str, volume_root: str, today: str, session: requests.Session | None = None):
    """Download the cumulative CSV as a dated snapshot. Returns ``(path, downloaded)``.

    The file is republished in place with all months (and revisions), so every real change is kept as
    its own ``snapshot=YYYY-MM-DD`` folder. If the size of the published file equals the latest snapshot,
    nothing is downloaded. (Size is a cheap check, not proof: a revision that keeps the byte size identical
    would be missed - the Silver ``_row_hash`` is the real revision detector.)
    """
    http = session or requests.Session()
    with http.get(url, stream=True, headers={"User-Agent": USER_AGENT}, timeout=(10, 600)) as resp:
        resp.raise_for_status()
        remote_size = int(resp.headers.get("Content-Length", -1))
        latest = latest_csv_snapshot(volume_root)
        if latest is not None and remote_size >= 0 and latest.stat().st_size == remote_size:
            return latest, False

        dest = csv_snapshots_dir(volume_root) / f"snapshot={today}" / CSV_NAME
        dest.parent.mkdir(parents=True, exist_ok=True)
        written = 0
        with open(dest, "wb") as fh:
            for chunk in resp.iter_content(CHUNK):
                fh.write(chunk)
                written += len(chunk)
    if remote_size >= 0 and written != remote_size:
        dest.unlink()
        raise IOError(f"{url}: expected {remote_size} bytes, wrote {written}")
    return dest, True
