# Delivery plan

Capacity: ~15–20 h/week. The Azure credits (€172) expired on 2026-10-29; no upgrade (ADR-018). Status below is as of **2026-11-03**.

| Dates | Phase | Deliverable | Done when | Status |
|---|---|---|---|---|
| Oct 17–18 | **0 · Source spike + Azure setup** | Sample AEAT files (one legacy month, one 2026-format month) inspected; Eurostat test query; resource group, ADLS Gen2, Databricks workspace with Unity Catalog, budget alerts | Open questions in `decisions.md` answered or re-scoped; go / change design | Done (two open items remain: Comext dataset, AEAT reuse terms) |
| Oct 19–25 | **1 · Vertical slice** | AEAT only, crude oil (2709), 2019→. Bronze → Silver → Gold (`energy_imports_monthly`, `supply_concentration`). `pytest` + GitHub Actions. Job run manually. One Power BI page. README with real results | Repo public; slice reproducible end-to-end | Partly: pipeline and tests done; no Power BI page, README results pending |
| Oct 26–29 | **2 · Differentiator** | `MERGE` + `_row_hash`, 2026 format, `revisions_summary`, quarantine, gas split (LNG vs pipeline), scheduled job | Historical backfill done **inside the credit window** | Partly: `MERGE`, row hash, 2026 format, LNG/pipeline split and backfill (2019-01 to 2026-08) done. Quarantine replaced by the price rule of ADR-016. `revisions_summary` built 2026-11-03 (offline). Scheduled job not done |
| Oct 28 | **Decision** | Upgrade to pay-as-you-go (monthly runs only, ≤ USD 20) **or** export Gold snapshot + `.pbix` and shut down | Taken 2026-10-29: no upgrade, Silver/Gold exported to CSV (ADR-018) |
| November | **3 · Production** | Automated scraper, Eurostat client, `eu_benchmark`, reconciliation warning, Asset Bundles, pipeline-health dashboard, final Power BI | v1 complete | Re-ordered below |
| Later | **v1.1** | Declarative-pipeline Gold; static interactive page | — | Not started |

Still open: how the project is presented in the README, CV and LinkedIn — decide at the end of Phase 1.

## Order of work from 2026-11-03
Reordered because the Azure workspace is gone (ADR-018): what can be built and checked without a workspace goes first, and the Databricks trial is started only when there is a concrete list to verify on it. If the trial turns out to be time-limited (to be confirmed), its clock is the constraint, so it is not started early.

| # | Step | Needs a workspace? | Done when |
|---|---|---|---|
| 1 | Docs: this plan, README status, ADR-018 and ADR-019 | No | Done 2026-11-03 |
| 2 | `revisions_summary` (Gold function, tests, notebook 04, offline script) | No | Done 2026-11-03: unit-tested, offline CSV matches an independent recount |
| 3 | Power BI Desktop (Import mode) on `data/gold_offline/` — Gold tables plus `dim_country` (ADR-020); business logic stays in Gold | No | `.pbix` + screenshots committed. Data side ready 2026-11-09; the report itself is built by hand in Desktop |
| 4 | README "Results" with numbers the pipeline produced (revisions, supply mix since 2022, format-break caveats of ADR-015/016) | No | No *pending* left in Results |
| 5 | Databricks trial, one planned pass: load Silver, run the ADR-017 `MERGE` (never run on Delta), build Gold incl. `revisions_summary` and `dim_country` (needs a notebook that reads the workbook from the volume), scheduled job, Asset Bundle, pipeline-health dashboard; record the trial's limits in ADR-018 | Yes | Screenshots / run evidence; README *pending* rows closed |
| 6 | Eurostat client, `eu_benchmark`, AEAT-vs-Eurostat reconciliation warning. First to cut (ADR-008). Comext is derived from AEAT-reported data, so it checks the pipeline, not the source | No (needs internet) | Dataset code resolved, tests green |
| 7 | v1.1 | — | — |

