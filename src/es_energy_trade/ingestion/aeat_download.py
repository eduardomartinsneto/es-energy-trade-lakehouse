"""Download AEAT ZIPs into a Unity Catalog volume and unpack the data file (Bronze, legacy format).

The ZIP is kept exactly as delivered; the unpacked fixed-width file sits next to it so Spark can read it.
Idempotent: a month already unpacked is skipped.
"""
from __future__ import annotations

import datetime
import hashlib
import json
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
#
# The file is republished in place with all months (and any revisions) and its URL and name change on every
# release, so each real change is kept as a dated snapshot folder with a manifest. Whether two downloads are
# "the same" is decided by SHA-256 of the content, never by size alone.

CSV_NAME = "MAXIMA_DESAGREGACION_2026.csv"  # fixed on-disk name, whatever the release called it
MANIFEST_NAME = "manifest.json"


def csv_snapshots_dir(volume_root: str) -> pathlib.Path:
    return pathlib.Path(volume_root) / "aeat" / "csv_2026"


def latest_csv_snapshot(volume_root: str) -> pathlib.Path | None:
    snaps = sorted(csv_snapshots_dir(volume_root).glob(f"snapshot=*/{CSV_NAME}"))
    return snaps[-1] if snaps else None


def sha256_of(path: pathlib.Path) -> str:
    digest = hashlib.sha256()
    with open(path, "rb") as fh:
        while chunk := fh.read(CHUNK):
            digest.update(chunk)
    return digest.hexdigest()


def read_manifest(snapshot: pathlib.Path) -> dict | None:
    f = snapshot.parent / MANIFEST_NAME
    return json.loads(f.read_text()) if f.exists() else None


def ensure_manifest(snapshot: pathlib.Path) -> dict:
    """Manifest of a snapshot; for one that was put there by hand (no manifest) it is built from the content."""
    manifest = read_manifest(snapshot)
    if manifest is None:
        manifest = {"url": None, "through_month": None, "size_bytes": snapshot.stat().st_size,
                    "sha256": sha256_of(snapshot), "etag": None, "last_modified": None,
                    "downloaded_at": None, "origin": "manual_upload"}
        (snapshot.parent / MANIFEST_NAME).write_text(json.dumps(manifest, indent=2))
    return manifest


def _same_release(manifest: dict, url: str, size: int, etag: str | None, last_modified: str | None) -> bool:
    """True only when the server's validators prove it is the file we already have.

    ETag or Last-Modified must be present on both sides and equal; size may only add a disagreement, never
    proof (two different releases can have the same byte size).
    """
    if manifest.get("url") != url:
        return False
    strong = [(manifest.get("etag"), etag), (manifest.get("last_modified"), last_modified)]
    strong = [(old, new) for old, new in strong if old is not None and new is not None]
    if not strong or any(old != new for old, new in strong):
        return False
    old_size = manifest.get("size_bytes")
    return old_size is None or size < 0 or old_size == size


def _new_snapshot_dir(volume_root: str, today: str) -> pathlib.Path:
    base = csv_snapshots_dir(volume_root)
    candidate, n = base / f"snapshot={today}", 1
    while candidate.exists():  # never overwrite a snapshot taken earlier the same day
        n += 1
        candidate = base / f"snapshot={today}-{n}"
    return candidate


def download_csv_snapshot(link, volume_root: str, today: str, session: requests.Session | None = None,
                          retries: int = 3, backoff_s: float = 5.0):
    """Download the cumulative CSV as a new snapshot. Returns ``(path, downloaded)``.

    * Skips the download when the server's ETag / Last-Modified / size prove the latest snapshot is this release.
    * Otherwise downloads (retrying transient failures), checks the byte count, hashes the content and
      writes a manifest. If the content is byte-identical to the latest snapshot, the new copy is discarded.
    * The Silver ``_row_hash`` remains the detector of row-level revisions.
    """
    import time

    if not hasattr(link, "url") or not hasattr(link, "through_month"):
        raise TypeError("download_csv_snapshot needs the CsvLink returned by find_csv_2026(), not its URL as text")
    http = session or requests.Session()
    last_error: Exception | None = None
    for attempt in range(1, retries + 1):
        try:
            return _download_once(http, link, volume_root, today)
        except (requests.RequestException, IOError) as exc:
            last_error = exc
            if attempt < retries:
                time.sleep(backoff_s * attempt)
    raise IOError(f"{link.url}: failed after {retries} attempts: {last_error!r}")


def _download_once(http, link, volume_root: str, today: str):
    latest = latest_csv_snapshot(volume_root)
    latest_manifest = ensure_manifest(latest) if latest is not None else None

    with http.get(link.url, stream=True, headers={"User-Agent": USER_AGENT}, timeout=(10, 600)) as resp:
        resp.raise_for_status()
        size = int(resp.headers.get("Content-Length", -1))
        etag, last_modified = resp.headers.get("ETag"), resp.headers.get("Last-Modified")
        if latest_manifest and _same_release(latest_manifest, link.url, size, etag, last_modified):
            return latest, False

        folder = _new_snapshot_dir(volume_root, today)
        folder.mkdir(parents=True, exist_ok=True)
        dest = folder / CSV_NAME
        digest, written = hashlib.sha256(), 0
        try:
            with open(dest, "wb") as fh:
                for chunk in resp.iter_content(CHUNK):
                    fh.write(chunk)
                    digest.update(chunk)
                    written += len(chunk)
            if size >= 0 and written != size:
                raise IOError(f"expected {size} bytes, wrote {written}")
        except Exception:
            dest.unlink(missing_ok=True)
            _remove_if_empty(folder)
            raise

    sha = digest.hexdigest()
    if latest_manifest and latest_manifest["sha256"] == sha:  # same bytes under a new URL or header: not a new release
        dest.unlink()
        _remove_if_empty(folder)
        return latest, False

    (folder / MANIFEST_NAME).write_text(json.dumps({
        "url": link.url, "through_month": link.through_month, "size_bytes": written, "sha256": sha,
        "etag": etag, "last_modified": last_modified,
        "downloaded_at": datetime.datetime.now(datetime.timezone.utc).isoformat(), "origin": "download",
    }, indent=2))
    return dest, True


def _remove_if_empty(folder: pathlib.Path) -> None:
    try:
        folder.rmdir()
    except OSError:
        pass
