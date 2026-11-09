"""Dimension tables built from AEAT reference data (workbook ``Clasi_new_fich.xlsx``)."""
from __future__ import annotations

import re
from collections.abc import Iterable

# Two capital letters, optionally followed by a footnote such as "MK  (1)".
_CODE = re.compile(r"^([A-Z]{2})(?:\W|$)")
# In sheet "Clasif- Países" every code after this row is a special code (UE, XI, XU, QP..QZ), not a country.
# The first letter is not a safe rule: QA is Qatar.
_SPECIAL_MARKER = "especiales"


def country_rows(sheet_rows: Iterable[tuple]) -> list[tuple[str, str, bool]]:
    """Rows of sheet ``Clasif- Países`` -> ``(country_code, country_name, is_country)``.

    Names are kept as AEAT publishes them (Spanish). ``is_country`` is False for the special codes listed after the
    "Códigos especiales" row (unspecified countries, ship and aircraft supplies, high seas, EU aggregates).
    Header and blank rows are skipped. A repeated code or a code without a name raises ``ValueError``.
    """
    out: list[tuple[str, str, bool]] = []
    seen: set[str] = set()
    is_country = True
    for row in sheet_rows:
        raw = row[0] if row else None
        if raw is None or not str(raw).strip():
            continue
        if _SPECIAL_MARKER in str(raw).lower():
            is_country = False
            continue
        match = _CODE.match(str(raw).replace("\xa0", " ").strip())
        if not match:
            continue  # the header row
        code = match.group(1)
        name = str(row[1]).strip() if len(row) > 1 and row[1] is not None else ""
        if not name:
            raise ValueError(f"country code {code} has no name")
        if code in seen:
            raise ValueError(f"country code {code} appears twice")
        seen.add(code)
        out.append((code, name, is_country))
    return out
