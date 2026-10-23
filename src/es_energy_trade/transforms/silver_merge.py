"""SQL for the Silver table and its MERGE (ADR-007 revision rule, ADR-012 key)."""
from __future__ import annotations

from es_energy_trade.transforms.aeat import SILVER_KEY

_SOURCE_COLS = ["flow", "period", "reporter_iso2", "cn8", "partner_code_src", "transport_mode",
                "source_format", "weight_kg", "value_eur", "supp_units", "_source_file", "_row_hash"]


def create_table_sql(target: str) -> str:
    return f"""
CREATE TABLE IF NOT EXISTS {target} (
  flow STRING NOT NULL,
  period DATE NOT NULL,
  reporter_iso2 STRING NOT NULL,
  cn8 STRING NOT NULL,
  partner_code_src STRING NOT NULL,
  transport_mode STRING NOT NULL,
  source_format STRING,
  weight_kg DECIMAL(18,3),
  value_eur DECIMAL(18,2),
  supp_units DECIMAL(18,3),
  _source_file STRING,
  _row_hash STRING,
  _first_loaded_at TIMESTAMP,
  _last_updated_at TIMESTAMP,
  _revision_count INT
) USING DELTA""".strip()


def merge_sql(target: str, source_view: str) -> str:
    """Insert new keys; update (and count a revision) only when the measures' hash changed.

    Identical re-deliveries match and change nothing, so they are not revisions.
    """
    on = " AND ".join(f"t.{c} = s.{c}" for c in SILVER_KEY)
    update = ",\n    ".join([
        "t.source_format = s.source_format",
        "t.weight_kg = s.weight_kg",
        "t.value_eur = s.value_eur",
        "t.supp_units = s.supp_units",
        "t._source_file = s._source_file",
        "t._row_hash = s._row_hash",
        "t._last_updated_at = current_timestamp()",
        "t._revision_count = t._revision_count + 1",
    ])
    cols = ", ".join(_SOURCE_COLS + ["_first_loaded_at", "_last_updated_at", "_revision_count"])
    vals = ", ".join([f"s.{c}" for c in _SOURCE_COLS] + ["current_timestamp()", "current_timestamp()", "0"])
    return f"""
MERGE INTO {target} AS t
USING {source_view} AS s
ON {on}
WHEN MATCHED AND t._row_hash <> s._row_hash THEN UPDATE SET
    {update}
WHEN NOT MATCHED THEN INSERT ({cols})
VALUES ({vals})""".strip()
