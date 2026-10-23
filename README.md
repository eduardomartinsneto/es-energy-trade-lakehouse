# Spain Energy Imports Lakehouse

> 🚧 **Work in progress** — design finished, implementation starting (October 2026). Sections marked *pending* will be filled only with results the pipeline actually produces.

**Question:** where does Spain import its crude oil and natural gas from, and how has that supply mix changed since 2022?

An end-to-end data engineering project on **Azure Databricks**: official Spanish customs files (AEAT) and the Eurostat API are ingested, harmonised and published as a small set of tested, quality-gated tables that feed a Power BI report.

## Why this project is interesting (engineering-wise)
- **Legacy + new formats:** AEAT publishes fixed-width monthly files and, since 2026, a new CSV format with continuous revisions of past months. The pipeline harmonises both.
- **Revisions handled explicitly:** official figures change after publication. Silver uses `MERGE` with row hashing, so real revisions are tracked (and reported) instead of silently overwritten.
- **Quality gates before publishing:** schema, key and sign checks fail the run; implausible values go to quarantine; cross-source reconciliation (AEAT vs Eurostat) raises warnings.
- **Orchestration as code:** Lakeflow Jobs defined with Databricks Asset Bundles, versioned in Git, transformation logic unit-tested with `pytest` in GitHub Actions.

## Architecture
```
AEAT website ──(Python scraper: discover monthly files)──┐
                                                          ├─► Bronze (raw files / raw API responses, ADLS + Delta)
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

## Stack
Azure Databricks · PySpark · Delta Lake · Unity Catalog · Lakeflow Jobs · Databricks Asset Bundles · ADLS Gen2 · Python (requests, BeautifulSoup) · pytest · GitHub Actions · Power BI

## Results
*Pending.*

## Repository layout
| Path | Content |
|---|---|
| `config/sources.yml` | Sources and parameters (metadata-driven ingestion) |
| `src/es_energy_trade/` | Ingestion, transformations and data-quality logic (unit-tested) |
| `notebooks/` | Thin Databricks notebooks that call `src/` |
| `tests/` | `pytest` suite |
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
