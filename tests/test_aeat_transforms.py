"""Parsing and Silver aggregation, checked against sums computed independently (awk) on the same fixtures."""
from datetime import date
from decimal import Decimal as D

import pytest
from pyspark.sql import functions as F

from es_energy_trade.ingestion.aeat_reader import read_2026, read_legacy
from es_energy_trade.transforms.aeat import SILVER_KEY, parse_2026, parse_legacy, to_silver

LEGACY = "aeat_legacy_ch27_sample.txt"  # mostly exports + one import
LEGACY_IMPORTS = "aeat_legacy_ch27_imports_sample.txt"  # real Jan-2025 import declarations
CSV_2026 = "aeat_2026_ch27_sample.csv"


def silver_rows(df, key_cols=("flow", "cn8", "partner_code_src", "transport_mode")):
    return {tuple(r[c] for c in key_cols): r for r in df.collect()}


# --- legacy ---------------------------------------------------------------------------------

def test_legacy_unit_conversion_and_fields(spark, fixtures_dir):
    row = parse_legacy(read_legacy(spark, str(fixtures_dir / LEGACY))).first()
    # line 1: raw weight 000015002110000 (3 implied decimals), raw value 000988789368 (euro cents)
    assert row.weight_kg == D("15002110.000")
    assert row.value_eur == D("9887893.68")
    assert row.period == date(2025, 1, 1)
    assert (row.cn8, row.partner_code_src, row.transport_mode) == ("27090090", "HR", "1")
    assert row.flow == "X"  # 'E' = export
    assert row.source_format == "legacy"


def test_legacy_import_flow_is_mapped_to_M(spark, fixtures_dir):
    flows = {r.flow for r in parse_legacy(read_legacy(spark, str(fixtures_dir / LEGACY_IMPORTS))).collect()}
    assert flows == {"M"}


def test_legacy_supplementary_units_have_three_implied_decimals(spark, fixtures_dir):
    silver = to_silver(parse_legacy(read_legacy(spark, str(fixtures_dir / LEGACY_IMPORTS))))
    qu = silver_rows(silver)[("M", "27112100", "QU", "8")]
    assert qu.supp_units == D("1.800")


def test_legacy_aggregation_matches_independent_sums(spark, fixtures_dir):
    silver = to_silver(parse_legacy(read_legacy(spark, str(fixtures_dir / LEGACY_IMPORTS))))
    rows = silver_rows(silver)
    assert len(rows) == 4

    mx = rows[("M", "27090090", "MX", "1")]
    assert (mx.weight_kg, mx.value_eur) == (D("794760570.000"), D("376247736.20"))

    ru = rows[("M", "27111100", "RU", "1")]
    assert (ru.weight_kg, ru.value_eur, ru.supp_units) == (D("424668802.000"), D("261501519.41"), D("23472.530"))

    dz = rows[("M", "27112100", "DZ", "7")]
    assert (dz.weight_kg, dz.value_eur, dz.supp_units) == (D("548299088.000"), D("270582037.44"), D("28279.829"))


def test_legacy_export_rows_aggregate_by_key(spark, fixtures_dir):
    silver = to_silver(parse_legacy(read_legacy(spark, str(fixtures_dir / LEGACY))))
    fr = silver_rows(silver)[("X", "27090090", "FR", "1")]  # 3 declarations
    assert (fr.weight_kg, fr.value_eur) == (D("25901724.000"), D("17891627.17"))


def test_legacy_layout_violation_fails_the_run(spark, tmp_path, fixtures_dir):
    lines = (fixtures_dir / LEGACY).read_text().splitlines()
    bad = tmp_path / "bad_legacy.txt"
    bad.write_text("\n".join(lines[:2] + [lines[2][:-1]]) + "\n")  # one line 225 chars long
    with pytest.raises(ValueError, match="legacy layout changed"):
        read_legacy(spark, str(bad))


def test_only_chapter_27_is_kept(spark, fixtures_dir, tmp_path):
    lines = (fixtures_dir / LEGACY).read_text().splitlines()
    plastics = lines[0][:25] + "39011090" + lines[0][33:]  # swap the CN8 (columns 26-33)
    f = tmp_path / "mixed.txt"
    f.write_text("\n".join([lines[0], plastics]) + "\n")
    parsed = parse_legacy(read_legacy(spark, str(f)))
    assert parsed.count() == 2
    assert to_silver(parsed).select("cn8").distinct().collect()[0].cn8 == "27090090"


# --- 2026 ------------------------------------------------------------------------------------

def test_2026_fields_and_cn8_truncation(spark, fixtures_dir):
    parsed = parse_2026(read_2026(spark, str(fixtures_dir / CSV_2026)))
    assert parsed.count() == 14
    lng = parsed.filter((F.col("partner_code_src") == "CG")).first()  # 2711110000 -> 27111100
    assert lng.cn8 == "27111100"
    assert (lng.weight_kg, lng.supp_units, lng.value_eur) == (D("58338625.000"), D("3197.960"), D("31308651.20"))
    assert lng.period == date(2026, 1, 1) and lng.flow == "M" and lng.source_format == "2026"


def test_2026_aggregation_sums_provinces_and_merges_cn8_and_cn10(spark, fixtures_dir):
    rows = silver_rows(to_silver(parse_2026(read_2026(spark, str(fixtures_dir / CSV_2026)))))

    it = rows[("M", "27090090", "IT", "1")]  # two province rows
    assert (it.weight_kg, it.value_eur) == (D("98180889.000"), D("30172701.10"))

    ao = rows[("M", "27090090", "AO", "1")]  # two rows, 10-digit CN
    assert (ao.weight_kg, ao.value_eur) == (D("259665281.000"), D("112038894.93"))

    ru = rows[("M", "27111100", "RU", "1")]
    assert (ru.weight_kg, ru.value_eur, ru.supp_units) == (D("286300201.000"), D("105626656.59"), D("15753.456"))

    dz = rows[("M", "27112100", "DZ", "7")]  # EUR and USD invoices already converted to statistical value
    assert (dz.weight_kg, dz.value_eur, dz.supp_units) == (D("459635177.000"), D("169227366.88"), D("23559.064"))


def test_2026_missing_transport_mode_gets_a_sentinel_key(spark, tmp_path):
    # real rows from the 2026 file: low-value residual CN 27000000 and an export with empty province / mode
    f = tmp_path / "missing_mode.csv"
    f.write_text(
        "I;2026;01;27000000;CN;;27.213;0;1016.21;;;TE;;\n"
        "I;2026;01;27000000;US;;0.399;0;3.39;;;TE;;\n"
        "E;2026;03;27101981;UZ;;415;0;7742.19;USD;;TE;10;28\n"
    )
    silver = to_silver(parse_2026(read_2026(spark, str(f))))
    rows = silver_rows(silver)
    assert set(rows) == {("M", "27000000", "CN", "NA"), ("M", "27000000", "US", "NA"), ("X", "27101981", "UZ", "NA")}
    assert rows[("M", "27000000", "CN", "NA")].weight_kg == D("27.213")


def test_2026_column_count_violation_fails_the_read(spark, tmp_path):
    f = tmp_path / "bad_2026.csv"
    f.write_text("I;2026;01;27090090;DE;48;22460;0;36605.31;;3;UE;11\n")  # 13 columns
    with pytest.raises(Exception):
        read_2026(spark, str(f)).collect()


# --- revision tracking -----------------------------------------------------------------------

def test_row_hash_is_stable_and_changes_only_with_the_measures(spark, fixtures_dir):
    path = str(fixtures_dir / CSV_2026)
    a = to_silver(parse_2026(read_2026(spark, path)))
    b = to_silver(parse_2026(read_2026(spark, path).withColumn("_source_file", F.lit("another_download.csv"))))
    joined = a.alias("a").join(b.alias("b"), SILVER_KEY)
    assert joined.count() == a.count()
    assert joined.filter(F.col("a._row_hash") != F.col("b._row_hash")).count() == 0  # same data, other file

    revised = read_2026(spark, path).withColumn(
        "_c8", F.when(F.col("_c4") == "IT", F.col("_c8").cast("decimal(18,2)") + 1).otherwise(F.col("_c8"))
    )
    c = to_silver(parse_2026(revised))
    diff = a.alias("a").join(c.alias("c"), SILVER_KEY).filter(F.col("a._row_hash") != F.col("c._row_hash"))
    assert {r.partner_code_src for r in diff.collect()} == {"IT"}
