# Delivery plan

Capacity: ~15–20 h/week. Azure credits (€172) expire **2026-10-29**.

| Dates | Phase | Deliverable | Done when |
|---|---|---|---|
| Oct 17–18 | **0 · Source spike + Azure setup** | Sample AEAT files (one legacy month, one 2026-format month) inspected; Eurostat test query; resource group, ADLS Gen2, Databricks workspace with Unity Catalog, budget alerts | Open questions in `decisions.md` answered or re-scoped; go / change design |
| Oct 19–25 | **1 · Vertical slice** | AEAT only, crude oil (2709), 2019→. Bronze → Silver → Gold (`energy_imports_monthly`, `supply_concentration`). `pytest` + GitHub Actions. Job run manually. One Power BI page. README with real results | Repo public; slice reproducible end-to-end |
| Oct 26–29 | **2 · Differentiator** | `MERGE` + `_row_hash`, 2026 format, `revisions_summary`, quarantine, gas split (LNG vs pipeline), scheduled job | Historical backfill done **inside the credit window** |
| Oct 28 | **Decision** | Upgrade to pay-as-you-go (monthly runs only, ≤ USD 20) **or** export Gold snapshot + `.pbix` and shut down | — |
| November | **3 · Production** | Automated scraper, Eurostat client, `eu_benchmark`, reconciliation warning, Asset Bundles, pipeline-health dashboard, final Power BI | v1 complete |
| Later | **v1.1** | Declarative-pipeline Gold; static interactive page | — |

Still open: how the project is presented in the README, CV and LinkedIn — decide at the end of Phase 1.
