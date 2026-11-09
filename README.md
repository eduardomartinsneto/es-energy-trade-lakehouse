# Spain Energy Imports Lakehouse

> 🚧 **Work in progress (v1, November 2026).** Sections marked *pending* will be filled only with results the pipeline actually produces.

## Status
| Implemented and unit-tested (`pytest`, GitHub Actions) | Pending for v1 |
|---|---|
| AEAT file discovery and download (legacy fixed-width and 2026 CSV formats) | Eurostat client, `eu_benchmark`, AEAT-vs-Eurostat reconciliation |
| Bronze → Silver parsers into a harmonised schema | Quarantine of implausible values (planned in ADR-008; only the price rule of ADR-016 exists) |
| Silver `MERGE` with row hashing; detection of changed and vanished rows | Power BI report |
| Quality gates: duplicate and null keys, negative values, period floor, release coverage, share of vanished rows | Pipeline-health dashboard |
| Gold: `energy_imports_monthly` (implied price withheld below 1,000 t per origin-month), `supply_concentration` (HHI, product-level price), `revisions_summary`, `dim_country` | Scheduled Lakeflow Job with Asset Bundles |
| Offline build of the Gold tables from exported data (`scripts/`) | `MERGE` with deletes (ADR-017) run on Delta: unit-tested only |

**Question:** where does Spain import its crude oil and natural gas from, and how has that supply mix changed since 2022?

**Environment.** The first runs (Silver and Gold, October 2026) were on an Azure Databricks workspace paid with trial credits that have ended. Further runs target a Databricks free trial account (see `docs/decisions.md`, ADR-018); until then the Gold tables are rebuilt offline from exported data.

An end-to-end data engineering project on **Databricks**: official Spanish customs files (AEAT) and the Eurostat API are ingested, harmonised and published as a small set of tested, quality-gated tables that feed a Power BI report.

## Why this project is interesting (engineering-wise)
- **Legacy + new formats:** AEAT publishes fixed-width monthly files and, since 2026, a new CSV format with continuous revisions of past months. The pipeline harmonises both.
- **Revisions handled explicitly:** official figures change after publication. Silver uses `MERGE` with row hashing, so real revisions are logged and summarised in `revisions_summary` instead of silently overwritten.
- **Quality gates before publishing:** schema, key and sign checks fail the run; implied prices of tiny flows are withheld instead of published (ADR-016). A quarantine and an AEAT-vs-Eurostat reconciliation warning are planned.
- **Tested transformations:** logic lives in `src/` as functions unit-tested with `pytest` in GitHub Actions; notebooks only orchestrate. Orchestration as code (Lakeflow Jobs with Databricks Asset Bundles) is planned, not done yet.

## Architecture
```
AEAT website ──(Python scraper: discover monthly files)──┐
                                                          ├─► Bronze (raw files / raw API responses, Unity Catalog volume + Delta)
Eurostat Comext API ──(Python client: filtered, paged)────┘
                                   │
                                   ▼
             Silver: trade_flows_aeat · trade_flows_eurostat · dim_country · dim_product
             (conformed schema, MERGE + _row_hash revision tracking)
                                   │   quality gates (fail / quarantine / warn)
                                   ▼
   Gold: energy_imports_monthly · supply_concentration (HHI) · eu_benchmark · revisions_summary
                                   │
                ┌──────────────────┴──────────────────┐
                ▼                                     ▼
     Power BI report (business)        Databricks AI/BI dashboard (pipeline health)

Orchestration: Lakeflow Jobs (Databricks Asset Bundles)
```
*Implemented so far: the AEAT branch, Silver, and the Gold tables `energy_imports_monthly`, `supply_concentration` and `revisions_summary`, and the `dim_country` dimension (built offline so far). Planned: the Eurostat branch, `eu_benchmark`, both dashboards and the Lakeflow Job.*

## Stack
Databricks (first runs on Azure Databricks) · PySpark · Delta Lake · Unity Catalog · Lakeflow Jobs · Databricks Asset Bundles · Python (requests, BeautifulSoup) · pytest · GitHub Actions · Power BI. Lakeflow Jobs, Asset Bundles and Power BI are planned, not yet implemented.

## Results
*Pending.*

## Repository layout
| Path | Content |
|---|---|
| `config/sources.yml` | Sources and parameters (metadata-driven ingestion) |
| `src/es_energy_trade/` | Ingestion, transformations and data-quality logic (unit-tested) |
| `notebooks/` | Thin Databricks notebooks that call `src/` |
| `tests/` | `pytest` suite |
| `scripts/` | Local helpers (offline Gold build from exported data) |
| `dashboards/powerbi/` | Power BI report and exports |
| `docs/decisions.md` | Architecture decision log (what was chosen, why, and what was rejected) |
| `docs/plan.md` | Delivery plan |

## Roadmap
- **v1:** AEAT ingestion → Gold, quality gates, revision tracking, Eurostat EU benchmark, Power BI report.
- **v1.1:** Gold rebuilt as a Lakeflow Declarative Pipeline (AUTO CDC + expectations) for comparison; static interactive page.
- **Later:** Iberian electricity market and retail fuel prices as additional domains, plugged in via `config/sources.yml`.

## Data sources
- Agencia Tributaria — *Estadísticas de Comercio Exterior* (monthly files).
- Eurostat — Comext database API.
