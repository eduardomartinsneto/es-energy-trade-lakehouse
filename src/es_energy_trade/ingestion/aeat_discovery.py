"""Discover the monthly AEAT "Máxima desagregación" ZIP files by walking the site.

What the site looks like (inspected 2026-10-20, see docs/decisions.md - Phase 1 findings):

* One page per year; it shows the ZIPs of ONE month and links to the other months' pages.
  The month shown differs by year (January for 2024-2026, December for 2019).
* Month sub-page URLs differ by year (``{year}-maxima-desagregacion/{mes}.html`` vs ``{year}/{mes}_.html``).
* The ZIP base path changed over time, and the month code in the file name is irregular
  (``cg25en39`` = Jan, ``cg25fb39`` = Feb, ``cg25sp39`` = Sep, ``cg25dc39`` = Dec).

So nothing is built from a pattern: links are read from the pages. The only contract used is
the file name ``cg<yy><mm-code><last-chapter>.zip`` and the Spanish month folder in its path.
Network access is injected (``fetch``) so the logic is testable without the site.
"""
from __future__ import annotations

import re
from dataclasses import dataclass
from typing import Callable, Iterable
from urllib.parse import urljoin

from bs4 import BeautifulSoup

YEAR_PAGE = "https://sede.agenciatributaria.gob.es/Sede/estadisticas/estadisticas-comercio-exterior/{year}-maxima-desagregacion.html"

MONTHS_ES = {
    "enero": 1, "febrero": 2, "marzo": 3, "abril": 4, "mayo": 5, "junio": 6,
    "julio": 7, "agosto": 8, "septiembre": 9, "octubre": 10, "noviembre": 11, "diciembre": 12,
}

# last chapter of the file: 22 -> chapters 01-22, 39 -> 23-39 (energy, chapter 27), ..., 99 -> 91-99
ENERGY_FILE_LAST_CHAPTER = "39"

_ZIP_NAME = re.compile(r"cg(?P<yy>\d{2})(?P<mcode>[a-z]{2})(?P<last>\d{2})\.zip$", re.IGNORECASE)


@dataclass(frozen=True)
class ZipLink:
    url: str
    year: int
    month: int
    month_code: str
    last_chapter: str
    file_name: str


def _month_from_path(url: str) -> int | None:
    for part in url.lower().split("/"):
        if part in MONTHS_ES:
            return MONTHS_ES[part]
    return None


_TEXT_MONTH = re.compile(r"^\s*(?P<month>[A-Za-z]+)\s+(?P<year>\d{4})\b")


def _month_from_text(text: str) -> int | None:
    """Link text looks like 'Julio 2022 54 al 61(13653 KB - zip ...)'."""
    m = _TEXT_MONTH.match(text)
    return MONTHS_ES.get(m["month"].lower()) if m else None


def parse_zip_links(html: str, page_url: str, default_month: int | None = None) -> list[ZipLink]:
    """ZIP links on a page that follow the ``cgYYMMNN.zip`` contract. Anything else is ignored.

    The month comes from the link text, then from the folder in the URL (the site is inconsistent: in July 2022
    the 54-61 file has no month folder, and the folder is sometimes capitalised), then from ``default_month``
    (the month of the sub-page being read). If sources disagree, or none gives a month, the link is dropped when
    it is not the energy file and raises when it is - an energy month we cannot place must stop the run.
    """
    links = []
    for a in BeautifulSoup(html, "html.parser").find_all("a", href=True):
        url = urljoin(page_url, a["href"].strip())
        m = _ZIP_NAME.search(url)
        if not m:
            continue
        found = {x for x in (_month_from_text(a.get_text(" ", strip=True)), _month_from_path(url)) if x}
        month = found.pop() if len(found) == 1 else (default_month if not found else None)
        if month is None:
            if m["last"] == ENERGY_FILE_LAST_CHAPTER:
                raise ValueError(f"Cannot determine the month of {url} (link text {a.get_text(' ', strip=True)!r})")
            continue
        links.append(
            ZipLink(
                url=url,
                year=2000 + int(m["yy"]),
                month=month,
                month_code=m["mcode"].lower(),
                last_chapter=m["last"],
                file_name=url.rsplit("/", 1)[-1],
            )
        )
    return links


def parse_month_links(html: str, page_url: str) -> dict[int, str]:
    """Links to the other months' pages, keyed by month number (taken from the link text)."""
    found: dict[int, str] = {}
    for a in BeautifulSoup(html, "html.parser").find_all("a", href=True):
        month = MONTHS_ES.get(a.get_text(strip=True).lower())
        if month and ".html" in a["href"]:
            found.setdefault(month, urljoin(page_url, a["href"].strip()))
    return found


def discover_year(year: int, fetch: Callable[[str], str]) -> list[ZipLink]:
    """All ZIP links of a year: the month shown on the year page plus every linked month page."""
    url = YEAR_PAGE.format(year=year)
    html = fetch(url)
    links = parse_zip_links(html, url)
    for month, month_url in parse_month_links(html, url).items():
        links.extend(parse_zip_links(fetch(month_url), month_url, default_month=month))
    # sanity: every file must belong to the requested year
    wrong = [link.url for link in links if link.year != year]
    if wrong:
        raise ValueError(f"Year {year}: links from another year found: {wrong[:3]}")
    return sorted(set(links), key=lambda link: (link.month, link.last_chapter))


def energy_files(links: Iterable[ZipLink]) -> list[ZipLink]:
    """The ZIP that contains CN chapter 27 (chapters 23-39), one per month."""
    selected = [link for link in links if link.last_chapter == ENERGY_FILE_LAST_CHAPTER]
    months = [link.month for link in selected]
    if len(months) != len(set(months)):
        raise ValueError("More than one chapter 23-39 file for the same month")
    return selected


# --- 2026 cumulative CSV ("nuevo formato") ------------------------------------------------------

CSV_2026_PAGE = "https://sede.agenciatributaria.gob.es/Sede/estadisticas/estadisticas-comercio-exterior/2026.html"
_CSV_NAME = re.compile(r"MAXIMA_DESAGREGACION_2026\.csv$", re.IGNORECASE)


@dataclass(frozen=True)
class CsvLink:
    url: str
    through_month: int  # last month the file covers, read from the folder in its path (".../julio/...")


def find_csv_2026(html: str, page_url: str = CSV_2026_PAGE) -> CsvLink:
    """The cumulative 2026 CSV link. Its folder is the last covered month, so the URL changes on every release."""
    urls = {
        urljoin(page_url, a["href"].strip())
        for a in BeautifulSoup(html, "html.parser").find_all("a", href=True)
        if _CSV_NAME.search(a["href"].strip())
    }
    if len(urls) != 1:
        raise ValueError(f"Expected exactly one MAXIMA_DESAGREGACION_2026.csv link, found {sorted(urls)}")
    url = urls.pop()
    month = _month_from_path(url)
    if month is None:
        raise ValueError(f"Cannot read the last covered month from the path of {url}")
    return CsvLink(url=url, through_month=month)


def missing_months(files: Iterable[ZipLink], start: tuple[int, int], end: tuple[int, int]) -> list[tuple[int, int]]:
    """(year, month) pairs in [start, end] that have no file - a gap must stop the backfill, not pass silently."""
    have = {(f.year, f.month) for f in files}
    missing, (y, m) = [], start
    while (y, m) <= end:
        if (y, m) not in have:
            missing.append((y, m))
        y, m = (y + 1, 1) if m == 12 else (y, m + 1)
    return missing
