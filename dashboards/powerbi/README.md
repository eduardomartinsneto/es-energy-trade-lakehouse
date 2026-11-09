# Power BI report — build notes

Source: the three Gold tables exported as CSV (`data/gold_offline/`, not in git). Import mode, one page, no business logic in DAX (ADR-009).

## Import (Power Query)
The CSVs use `.` as decimal separator and some numbers are in scientific notation (`5.227e-05`). A Desktop installed with a Spanish or Portuguese locale will misread them unless the culture is forced. Use a text parameter `DataFolder` (path ending in a separator) and, for each table:

```
let
  Source   = Csv.Document(File.Contents(DataFolder & "energy_imports_monthly.csv"), [Delimiter = ",", Encoding = 65001, QuoteStyle = QuoteStyle.Csv]),
  Promoted = Table.PromoteHeaders(Source, [PromoteAllScalars = true]),
  Typed    = Table.TransformColumnTypes(Promoted, {
               {"period", type date}, {"product", type text}, {"origin_code", type text},
               {"weight_kg", type number}, {"value_eur", type number},
               {"implied_price_eur_per_t", type number}, {"share_of_weight", type number}}, "en-US")
in Typed
```
Same pattern for `supply_concentration.csv` and `revisions_summary.csv` (`run_at` as `datetime`, `period` as `date`).

## Reconcile after loading (if these do not match, the import is wrong)
| Table | Rows | Check |
|---|---|---|
| `energy_imports_monthly` | 2,935 | 348 blank `implied_price_eur_per_t` (blank, not errors); periods 2019-01 to 2026-08; 3 products |
| `supply_concentration` | 276 | no blank `implied_price_eur_per_t` in the current data |
| `revisions_summary` | 32 | `n_revised` sums to 195; `abs_value_delta_eur` for `flow = M` sums to about 649.0 M EUR |
| `dim_country` | 253 | `country_code` unique; 89 of the codes appear in `energy_imports_monthly[origin_code]`, 3 of them with `is_country = false` (`QU`, `QV`, `QW`) |

CSVs use fixed notation (no `1e-05`) and `.` as decimal separator; keep the `en-US` culture in the type step.

## Model
- One relationship: `dim_country[country_code]` (one) → `energy_imports_monthly[origin_code]` (many), single direction. Use `country_name` in visuals, never the code.
- Decide how to treat `is_country = false` (unspecified countries): show them as one "Not specified" bucket or filter them out; do not leave them mixed in with countries without saying so.
- Product-level price: use `supply_concentration[implied_price_eur_per_t]` as a column. Do not average the origin-level price, and do not compute a ratio in DAX (ADR-009, ADR-020).
- Names are in Spanish as AEAT publishes them.

## Limits to state in the report
- LNG origin may be provenance (ADR-008); pipeline gas is not comparable across January 2026 (ADR-015 findings); September 2025 pipeline gas is unexplained; the latest months are revised for at least six months (`revisions_summary`).
