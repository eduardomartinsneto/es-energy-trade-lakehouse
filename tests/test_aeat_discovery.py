"""Discovery logic against HTML *modelled on* the AEAT pages (hrefs copied from the real pages, 2026-10-20).

The pages are synthetic: only the structure we rely on (anchors with hrefs and month names) is reproduced.
"""
import pytest

from es_energy_trade.ingestion.aeat_discovery import (
    discover_year, energy_files, parse_month_links, parse_zip_links,
)

NEW = "https://sede.agenciatributaria.gob.es/static_files/Sede/Tema/Aduanas/Comercio_exterior/maxima_desag_mens"
OLD = ("https://sede.agenciatributaria.gob.es/static_files/AEAT/Aduanas/Contenidos_Privados/"
       "Estadisticas_Comercio_Exterior/comercio_exterior/datos_mensuales_maxima_desagregacion")
SITE = "https://sede.agenciatributaria.gob.es/Sede/estadisticas/estadisticas-comercio-exterior"
LAST = ["22", "39", "53", "61", "63", "74", "84", "86", "90", "99"]
MONTH_NAMES = ["Enero", "Febrero", "Marzo", "Abril", "Mayo", "Junio", "Julio", "Agosto",
               "Septiembre", "Octubre", "Noviembre", "Diciembre"]


def zips(base, year, folder, code):
    yy = str(year)[2:]
    return "".join(f'<a href="{base}/{year}/{folder}/cg{yy}{code}{n}.zip">{folder} {n}</a>' for n in LAST)


def nav(pattern, year, skip):
    return "".join(f'<li><a href="{pattern.format(year=year, mes=m.lower())}">{m}</a></li>'
                   for m in MONTH_NAMES if m != skip)


def fake_site():
    new_pat = SITE + "/{year}-maxima-desagregacion/{mes}.html"
    old_pat = SITE + "/{year}/{mes}_.html"
    return {
        # 2025: year page shows January, sub-pages for the rest (irregular month codes: en, fb)
        f"{SITE}/2025-maxima-desagregacion.html": zips(NEW, 2025, "enero", "en") + nav(new_pat, 2025, "Enero"),
        f"{SITE}/2025-maxima-desagregacion/febrero.html": zips(NEW, 2025, "febrero", "fb"),
        # 2019: year page shows December, other pattern for sub-pages and other base path
        f"{SITE}/2019-maxima-desagregacion.html": zips(OLD, 2019, "diciembre", "dc") + nav(old_pat, 2019, "Diciembre"),
        f"{SITE}/2019/enero_.html": zips(OLD, 2019, "enero", "en"),
    }


def fetch_from(pages):
    def fetch(url):
        if url not in pages:
            return ""  # months not modelled in this test
        return pages[url]
    return fetch


def test_zip_links_read_year_month_and_chapter_from_the_url():
    html = zips(NEW, 2025, "septiembre", "sp")  # 'sp' is the real code for September
    links = parse_zip_links(html, SITE + "/x.html")
    assert len(links) == 10
    l = [x for x in links if x.last_chapter == "39"][0]
    assert (l.year, l.month, l.month_code, l.file_name) == (2025, 9, "sp", "cg25sp39.zip")
    assert l.url == f"{NEW}/2025/septiembre/cg25sp39.zip"


def test_non_data_links_are_ignored():
    html = '<a href="/static/diseno226.pdf">Formato</a><a href="/x/Nota_datos_septiembre_INTRSTT.pdf">Nota</a>'
    assert parse_zip_links(html, SITE) == []


def test_month_links_use_the_link_text_and_resolve_relative_urls():
    html = '<a href="/Sede/a/2025-maxima-desagregacion/febrero.html">Febrero</a><a href="/other.html">Inicio</a>'
    assert parse_month_links(html, SITE) == {2: "https://sede.agenciatributaria.gob.es/Sede/a/2025-maxima-desagregacion/febrero.html"}


def test_discover_year_walks_year_page_and_month_pages():
    links = discover_year(2025, fetch_from(fake_site()))
    assert {l.month for l in links} == {1, 2}
    assert {l.month_code for l in links} == {"en", "fb"}
    assert len(links) == 20


def test_discover_year_handles_the_2019_layout():
    links = discover_year(2019, fetch_from(fake_site()))
    assert {l.month for l in links} == {1, 12}
    assert all(l.url.startswith(OLD) for l in links)


def test_energy_files_keep_only_chapters_23_to_39_one_per_month():
    sel = energy_files(discover_year(2025, fetch_from(fake_site())))
    assert [(l.month, l.file_name) for l in sel] == [(1, "cg25en39.zip"), (2, "cg25fb39.zip")]


def test_a_link_from_another_year_fails_discovery():
    pages = fake_site()
    pages[f"{SITE}/2025-maxima-desagregacion/febrero.html"] = zips(NEW, 2024, "febrero", "fb")
    with pytest.raises(ValueError, match="another year"):
        discover_year(2025, fetch_from(pages))


def test_two_energy_files_for_one_month_fail():
    links = parse_zip_links(zips(NEW, 2025, "enero", "en") + zips(NEW, 2025, "enero", "xx"), SITE + "/x.html")
    with pytest.raises(ValueError, match="same month"):
        energy_files(links)


def test_csv_2026_link_and_last_covered_month():
    from es_energy_trade.ingestion.aeat_discovery import find_csv_2026
    html = ('<a href="https://sede.agenciatributaria.gob.es/static_files/Sede/Tema/Aduanas/Comercio_exterior/'
            'Nuevo_formato_2026_max_des/julio/MAXIMA_DESAGREGACION_2026.csv">Enero a Julio</a>')
    link = find_csv_2026(html)
    assert link.through_month == 7
    assert link.url.endswith("/julio/MAXIMA_DESAGREGACION_2026.csv")


def test_csv_2026_link_missing_or_ambiguous_fails():
    from es_energy_trade.ingestion.aeat_discovery import find_csv_2026
    with pytest.raises(ValueError, match="exactly one"):
        find_csv_2026("<a href='/x.pdf'>nada</a>")


def test_missing_months_reports_gaps_in_the_requested_range():
    from es_energy_trade.ingestion.aeat_discovery import missing_months
    files = energy_files(discover_year(2025, fetch_from(fake_site())))  # Jan and Feb 2025 only
    assert missing_months(files, (2025, 1), (2025, 2)) == []
    assert missing_months(files, (2024, 12), (2025, 3)) == [(2024, 12), (2025, 3)]


def test_month_comes_from_link_text_when_the_url_has_no_month_folder():
    # Real case (July 2022): the 54-61 file has no month folder; others have a capitalised 'Julio' folder.
    base = OLD + "/2022"
    html = (
        f'<a href="{base}/Julio/cg22jl39.zip">Julio 2022 23 al 39(12905 KB - zip (Archivo comprimido))</a>'
        f'<a href="{base}/cg22jl61.zip">Julio 2022 54 al 61(13653 KB - zip (Archivo comprimido))</a>'
    )
    links = {l.last_chapter: l for l in parse_zip_links(html, SITE + "/x.html")}
    assert links["39"].month == 7 and links["61"].month == 7
    assert links["61"].month_code == "jl"


def test_unplaceable_non_energy_link_is_dropped_but_an_energy_one_raises():
    base = OLD + "/2022"
    assert parse_zip_links(f'<a href="{base}/cg22jl61.zip">archivo</a>', SITE + "/x.html") == []
    with pytest.raises(ValueError, match="Cannot determine the month"):
        parse_zip_links(f'<a href="{base}/cg22jl39.zip">archivo</a>', SITE + "/x.html")


def test_default_month_of_the_subpage_places_a_link_without_other_clues():
    base = OLD + "/2022"
    links = parse_zip_links(f'<a href="{base}/cg22jl39.zip">archivo</a>', SITE + "/julio.html", default_month=7)
    assert [l.month for l in links] == [7]


def test_conflicting_month_sources_are_not_trusted():
    base = OLD + "/2022"
    html = f'<a href="{base}/Julio/cg22jl39.zip">Agosto 2022 23 al 39(1 KB)</a>'
    with pytest.raises(ValueError, match="Cannot determine the month"):
        parse_zip_links(html, SITE + "/x.html")
