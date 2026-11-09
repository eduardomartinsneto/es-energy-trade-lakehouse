import pytest

from es_energy_trade.transforms.dim import country_rows

SHEET = [
    ("Código", "Texto", "Descripción"),
    ("AD ", "Andorra", None),
    ("MK \xa0(1)", "Antigua República Yugoslava de Macedonia", None),   # footnote marker inside the code cell
    ("QA", "Qatar", None),                                              # starts with Q but is a country
    (None, None, None),
    ("Códigos especiales", None, None),
    ("QU", "Países y territorios no especificados", None),
    ("UE", "Unión Europea", None),
]


def test_country_rows_cleans_codes_and_flags_special_codes_by_position_not_by_letter():
    assert country_rows(SHEET) == [
        ("AD", "Andorra", True),
        ("MK", "Antigua República Yugoslava de Macedonia", True),
        ("QA", "Qatar", True),
        ("QU", "Países y territorios no especificados", False),
        ("UE", "Unión Europea", False),
    ]


def test_country_rows_rejects_duplicates_and_missing_names():
    with pytest.raises(ValueError, match="twice"):
        country_rows([("AD", "Andorra"), ("AD ", "Andorra")])
    with pytest.raises(ValueError, match="no name"):
        country_rows([("AD", None)])
