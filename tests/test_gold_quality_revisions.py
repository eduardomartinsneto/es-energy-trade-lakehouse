from datetime import date
from decimal import Decimal as D

import pytest
from pyspark.sql import functions as F

from es_energy_trade.quality.gates import QualityGateError, check_silver
from es_energy_trade.transforms.aeat import SILVER_KEY, parse_2026, to_silver
from es_energy_trade.transforms.gold import energy_imports_monthly, revisions_summary, supply_concentration
from es_energy_trade.transforms.revisions import changed_rows, vanished_rows
from es_energy_trade.transforms.silver_merge import merge_sql

COLS = ["flow", "period", "reporter_iso2", "cn8", "partner_code_src", "transport_mode",
        "weight_kg", "value_eur", "supp_units", "_source_file", "_row_hash"]
P = date(2025, 1, 1)


def silver_df(spark, rows):
    """rows: (flow, period, cn8, partner, mode, weight_kg, value_eur)."""
    data = [(f, p, "ES", cn, partner, mode, D(str(w)), D(str(v)), D("0"), "f", "h")
            for f, p, cn, partner, mode, w, v in rows]
    return spark.createDataFrame(data, ", ".join(
        f"{c} {t}" for c, t in zip(COLS, ["string", "date", "string", "string", "string", "string",
                                          "decimal(18,3)", "decimal(18,2)", "decimal(18,3)", "string", "string"])))


# --- gold ------------------------------------------------------------------------------------

@pytest.fixture
def silver(spark):
    return silver_df(spark, [
        ("M", P, "27090090", "A", "1", 40_000_000, 20_000_000),   # origin A: two transport modes, summed
        ("M", P, "27090010", "A", "3", 20_000_000, 10_000_000),
        ("M", P, "27090090", "B", "1", 30_000_000, 18_000_000),
        ("M", P, "27090090", "C", "1", 10_000_000, 4_000_000),
        ("X", P, "27090090", "Z", "1", 99_000_000, 99_000_000),   # export: ignored
        ("M", P, "27101981", "Y", "1", 50_000_000, 25_000_000),   # refined product: no product, ignored
        ("M", P, "27000000", "CN", "NA", 27, 1016),               # residual CN: ignored
        ("M", P, "27112100", "DZ", "7", 5_000_000, 2_000_000),    # pipeline gas
    ])


def test_energy_imports_monthly(silver):
    rows = {(r.product, r.origin_code): r for r in energy_imports_monthly(silver).collect()}
    assert set(rows) == {("crude_oil", "A"), ("crude_oil", "B"), ("crude_oil", "C"), ("pipeline_gas", "DZ")}

    a = rows[("crude_oil", "A")]
    assert (a.weight_kg, a.value_eur) == (D("60000000.000"), D("30000000.00"))
    assert a.implied_price_eur_per_t == D("500.00")           # 30,000,000 EUR / 60,000 t
    assert a.share_of_weight == pytest.approx(0.6)             # 60 of 100 kt
    assert rows[("crude_oil", "B")].implied_price_eur_per_t == D("600.00")
    assert rows[("pipeline_gas", "DZ")].share_of_weight == pytest.approx(1.0)


def test_implied_price_needs_a_minimum_volume(spark):
    """ADR-016: below 1,000 t per origin-month the price is NULL; weight, value and shares stay."""
    df = silver_df(spark, [
        ("M", P, "27090090", "BIG", "1", 1_000_000, 500_000),     # exactly 1,000 t: reported
        ("M", P, "27090090", "TINY", "1", 999_999, 5_000_000),    # 999.999 t at an absurd price: withheld
    ])
    rows = {r.origin_code: r for r in energy_imports_monthly(df).collect()}
    assert rows["BIG"].implied_price_eur_per_t == D("500.00")
    assert rows["TINY"].implied_price_eur_per_t is None
    assert rows["TINY"].value_eur == D("5000000.00")           # the flow is still counted
    assert rows["TINY"].share_of_weight == pytest.approx(0.499999750000125)
    assert energy_imports_monthly(df, min_weight_for_price_kg=0).filter("origin_code='TINY'").first() \
        .implied_price_eur_per_t is not None


def test_supply_concentration(silver):
    conc = {r.product: r for r in supply_concentration(energy_imports_monthly(silver)).collect()}
    crude = conc["crude_oil"]
    assert crude.hhi == pytest.approx(4600.0)                  # (0.6^2 + 0.3^2 + 0.1^2) * 10,000
    assert crude.top1_share == pytest.approx(0.6)
    assert crude.n_origins == 3
    assert crude.total_weight_kg == D("100000000.000")
    assert conc["pipeline_gas"].hhi == pytest.approx(10000.0)  # single origin = maximum concentration
    assert crude.implied_price_eur_per_t == D("520.00")        # EUR 52 M over 100,000 t, not an average of the origin prices
    assert conc["pipeline_gas"].implied_price_eur_per_t == D("400.00")


def test_product_price_is_withheld_below_the_minimum_volume(spark):
    small = silver_df(spark, [("M", P, "27111100", "DZ", "1", 500_000, 200_000)])   # 500 t in the month
    row = supply_concentration(energy_imports_monthly(small)).collect()[0]
    assert row.total_weight_kg == D("500000.000") and row.implied_price_eur_per_t is None


# --- quality gates ---------------------------------------------------------------------------

def test_gate_passes_on_a_clean_batch(silver):
    assert check_silver(silver)["rows"] == 8


def test_gate_fails_on_duplicate_keys(spark):
    df = silver_df(spark, [("M", P, "27090090", "A", "1", 1, 1), ("M", P, "27090090", "A", "1", 2, 2)])
    with pytest.raises(QualityGateError, match="duplicate_keys"):
        check_silver(df)


def test_gate_fails_on_null_key_and_negative_values(spark):
    df = silver_df(spark, [("M", P, "27090090", "A", "1", -1, 1)])
    with pytest.raises(QualityGateError, match="negative_measures"):
        check_silver(df)
    with pytest.raises(QualityGateError, match="null_keys"):
        check_silver(df.withColumn("transport_mode", F.lit(None).cast("string")))


# --- revisions -------------------------------------------------------------------------------

def test_changed_rows_reports_only_real_revisions(spark, fixtures_dir):
    from es_energy_trade.ingestion.aeat_reader import read_2026
    base = to_silver(parse_2026(read_2026(spark, str(fixtures_dir / "aeat_2026_ch27_sample.csv"))))

    # a new release: Italy's crude revised (+1 EUR), everything else identical, one brand-new key
    revised = base.withColumn("value_eur", F.when(F.col("partner_code_src") == "IT",
                                                  F.col("value_eur") + 1).otherwise(F.col("value_eur")))
    revised = revised.withColumn("_row_hash", F.sha2(F.concat_ws("|", F.col("weight_kg").cast("string"),
                                                                 F.col("value_eur").cast("string"),
                                                                 F.col("supp_units").cast("string")), 256))
    new_key = revised.limit(1).withColumn("partner_code_src", F.lit("ZZ"))
    release = revised.unionByName(new_key)

    out = changed_rows(release, base).collect()
    assert len(out) == 1                                        # the new key (ZZ) is an insert, not a revision
    r = out[0]
    assert r.partner_code_src == "IT"
    assert r.value_eur_new - r.value_eur_old == D("1.00")
    assert r._row_hash_old != r._row_hash_new


# --- merge SQL -------------------------------------------------------------------------------

def test_merge_sql_uses_the_adr012_key_and_counts_revisions_only_on_hash_change():
    sql = merge_sql("portfolio.silver.trade_flows_aeat", "batch")
    for col in SILVER_KEY:
        assert f"t.{col} = s.{col}" in sql
    assert "WHEN MATCHED AND t._row_hash <> s._row_hash" in sql
    assert "t._revision_count = t._revision_count + 1" in sql
    assert "source_format" not in sql.split("ON ", 1)[1].split("WHEN", 1)[0]   # not part of the key


def test_period_floor_gate(spark):
    from es_energy_trade.quality.gates import check_period_floor
    ok = silver_df(spark, [("M", date(2026, 1, 1), "27090090", "A", "1", 1, 1)])
    check_period_floor(ok, "2026-01-01")
    early = silver_df(spark, [("M", date(2025, 12, 1), "27090090", "A", "1", 1, 1)])
    with pytest.raises(QualityGateError, match="overlaps the legacy history"):
        check_period_floor(early, "2026-01-01")


def test_release_coverage_gate(spark):
    from es_energy_trade.quality.gates import check_release_coverage
    months = lambda *ms: silver_df(spark, [("M", date(2026, m, 1), "27090090", "A", "1", 1, 1) for m in ms])
    assert check_release_coverage(months(1, 2, 3), claimed_through_month=3) == date(2026, 3, 1)
    with pytest.raises(QualityGateError, match="missing months"):          # a fragment, not a cumulative file
        check_release_coverage(months(8))
    with pytest.raises(QualityGateError, match="missing months"):
        check_release_coverage(months(1, 3))
    with pytest.raises(QualityGateError, match="claims to end in month 8"):
        check_release_coverage(months(1, 2, 3), claimed_through_month=8)


# --- vanished keys (ADR-017) -----------------------------------------------------------------

def _with_hash_and_format(df, fmt):
    return (df.withColumn("source_format", F.lit(fmt))
              .withColumn("_row_hash", F.sha2(F.concat_ws("|", F.col("weight_kg").cast("string"), F.col("value_eur").cast("string")), 256)))


def test_vanished_rows_are_target_keys_missing_from_a_full_release_of_the_same_format(spark):
    target = _with_hash_and_format(silver_df(spark, [
        ("M", P, "27111100", "MR", "1", 1_000_000, 500_000),
        ("M", P, "27111100", "DZ", "1", 2_000_000, 900_000),
    ]), "2026").unionByName(_with_hash_and_format(silver_df(spark, [
        ("M", date(2024, 1, 1), "27090090", "US", "1", 3_000_000, 1_000_000),      # legacy history: never touched
    ]), "legacy"))
    release = _with_hash_and_format(silver_df(spark, [("M", P, "27111100", "DZ", "1", 2_000_000, 900_000)]), "2026")

    out = vanished_rows(release, target, "2026").collect()
    assert [(r.partner_code_src, r._change_type, r.weight_kg_new) for r in out] == [("MR", "vanished", None)]
    assert out[0].weight_kg_old == D("1000000.000")


def test_merge_sql_deletes_missing_rows_only_when_asked():
    plain = merge_sql("t", "b")
    assert "NOT MATCHED BY SOURCE" not in plain
    scoped = merge_sql("t", "b", "t.source_format = '2026'")
    assert scoped.endswith("WHEN NOT MATCHED BY SOURCE AND t.source_format = '2026' THEN DELETE")


def test_vanished_share_gate():
    from es_energy_trade.quality.gates import check_vanished_share
    check_vanished_share(9, 14_366)                                   # 0.06 %
    with pytest.raises(QualityGateError, match="truncated"):
        check_vanished_share(500, 14_366)


# --- revisions_summary -----------------------------------------------------------------------

M26 = date(2026, 5, 1)


@pytest.fixture
def revisions_and_silver(spark):
    """One run (2026-10-28) over May 2026: a revised crude row, a vanished LNG row, a revised refined-product row."""
    target = _with_hash_and_format(silver_df(spark, [
        ("M", M26, "27090090", "IT", "1", 10_000_000, 5_000_000),
        ("M", M26, "27090090", "US", "1", 20_000_000, 10_000_000),     # unchanged: not a revision
        ("M", M26, "27111100", "MR", "1", 1_000_000, 500_000),         # absent from the release: vanished
        ("M", M26, "27101981", "BR", "1", 1_000_000, 300_000),         # refined product -> product "other"
    ]), "2026")
    release = _with_hash_and_format(silver_df(spark, [
        ("M", M26, "27090090", "IT", "1", 10_100_000, 5_500_000),      # +100 t, +EUR 500,000
        ("M", M26, "27090090", "US", "1", 20_000_000, 10_000_000),
        ("M", M26, "27101981", "BR", "1", 1_000_000, 250_000),         # -EUR 50,000
    ]), "2026")
    log = changed_rows(release, target).unionByName(vanished_rows(release, target, "2026"))
    log = log.withColumn("_run_at", F.lit("2026-10-28 22:59:33").cast("timestamp"))
    return log, release            # the release is what Silver holds after the MERGE


def test_revisions_summary_counts_sizes_and_lag(revisions_and_silver):
    log, silver_after = revisions_and_silver
    rows = {r.product: r for r in revisions_summary(log, silver=silver_after).collect()}
    assert set(rows) == {"crude_oil", "lng", "other"}

    crude = rows["crude_oil"]
    assert (crude.n_revised, crude.n_vanished, crude.months_after_period) == (1, 0, 5)   # Oct run, May data
    assert crude.value_delta_eur == D("500000.00") and crude.weight_delta_kg == D("100000.000")
    assert crude.period_value_eur == D("15500000.00")
    assert crude.abs_value_delta_share_of_month == pytest.approx(500_000 / 15_500_000)

    lng = rows["lng"]                                         # vanished: the whole old value is lost
    assert (lng.n_revised, lng.n_vanished) == (0, 1)
    assert lng.value_delta_eur == D("-500000.00") and lng.abs_value_delta_eur == D("500000.00")
    assert lng.abs_value_delta_share_of_month is None         # nothing left in the month to compare against

    other = rows["other"]                                     # negative revisions count by absolute size
    assert other.value_delta_eur == D("-50000.00") and other.abs_value_delta_eur == D("50000.00")
    assert other.abs_value_delta_share_of_month == pytest.approx(0.2)


def test_revisions_summary_handles_logs_without_change_type_and_without_silver(revisions_and_silver):
    log, _ = revisions_and_silver
    old_style = log.filter(F.col("_change_type") == "revised").drop("_change_type")   # rows logged before ADR-017
    rows = revisions_summary(old_style).collect()
    assert sum(r.n_revised for r in rows) == 2 and sum(r.n_vanished for r in rows) == 0
    assert all(r.abs_value_delta_share_of_month is None for r in rows)
