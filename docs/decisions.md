# Architecture Decision Log

Decisions taken on 2026-10-16 during the design review. Each entry records what was chosen, why, and what was rejected — so the trade-offs can be defended in an interview. Change a decision by adding a new entry, not by editing an old one.

---

## ADR-001 — Purpose and audience
**Decision.** Engineering-first portfolio project, aimed at the *technical interviewer* (screening recruiters rarely open GitHub). It must prove what the CV does not: Python/PySpark written by the author, a Databricks lakehouse (Delta, Unity Catalog), incremental loads with revisions, data quality as a publishing contract, orchestration as code. It ends in a business report.
**Why.** Python appears as a requirement in most target job ads; Databricks in a large share. Azure Data Factory is already backed by six years of professional experience.
**Rejected.** A tool tour (every tool used once "to tick the box"); an analysis-first project (BI-heavy, thin pipeline).

## ADR-002 — Topic and business question
**Decision.** Spain's imports of crude oil (CN 2709) and natural gas (CN 2711: LNG vs pipeline) by origin, monthly, from 2019. Question: *where does Spain import them from, and how has the mix changed since 2022?*
**Why.** Continuity with prior professional work on energy trade and price data; official trade statistics are revised after publication, which forces real incremental/MERGE logic; relevant to energy-sector employers in Spain. 2019 gives a pre-Covid / Covid / 2022-crisis baseline.
**Rejected.** Football (crowded portfolio topic, scraping ToS issues, no link to experience); aviation (pulls towards streaming, no domain link). Iberian electricity market and retail fuel prices are deferred as future domains.

## ADR-003 — Sources
**Decision.**
- **AEAT "Máxima desagregación" monthly files** — primary source for Spain (it is the upstream of Spain's figures in Eurostat). Legacy fixed-width layout plus the new 2026 CSV format with continuous revisions.
- **Eurostat Comext API** — only for the EU benchmark (ES, PT, IT, FR, DE, same CN codes) and for AEAT-vs-Eurostat reconciliation on Spain.
- **AEAT classifications workbook** — product dimension (CN descriptions).
**Why.** Each source has a distinct role: file ingestion + scraping (AEAT), REST API in Python (Eurostat). Year pages on the AEAT site follow different URL patterns (2016–2018 vs 2019+), so link discovery is a legitimate scraping use case.
**Rejected.** CORES (redundant in v1; revisit if gas volumes in energy units are needed); Eurostat `nrg_ti_*` energy datasets (redundant); Eurostat as the source for Spain (duplicates the upstream).

## ADR-004 — Environment and budget
**Decision.** Azure Databricks + ADLS Gen2 on a personal Azure subscription. Budget ceiling **USD 20/month** once credits end; current credits (€172) expire **2026-10-29**. Budget alerts at 10 and 20; every cluster auto-terminates; single-node job clusters.
**Why.** Databricks Free Edition restricts outbound internet to an undisclosed list of trusted domains (risk for the scraper and the API), is non-commercial and does not support custom storage.
**Rejected.** Free Edition only; hybrid ADF + Free Edition (awkward integration, hard to defend).
**Note.** Budget alerts notify, they do not stop spend.

## ADR-005 — Orchestration
**Decision.** **Lakeflow Jobs**, defined as code with Databricks Asset Bundles. Azure Data Factory is **not** used.
**Why.** Orchestration lives where the logic lives, versioned in Git; jobs are part of the Databricks Data Engineer Associate syllabus. ADF skills are already evidenced elsewhere.
**Rejected.** ADF as master orchestrator; a thin ADF layer triggering the job via the *Databricks Job* activity (looked decorative).
**Interview answer to "why not ADF?"** "I kept orchestration in the platform that runs the logic, as code in Git. In an enterprise Azure setup, ADF could trigger this job through its Databricks Job activity."

## ADR-006 — Transformation style
**Decision.** Imperative PySpark with an explicit `MERGE INTO`; transformations as pure functions in `src/`, unit-tested with `pytest` in GitHub Actions. **v1.1:** rebuild the Gold layer as a Lakeflow Declarative Pipeline (AUTO CDC + expectations) and compare.
**Why.** The declarative approach would hide exactly the skills the project must prove (MERGE semantics, incremental logic) and is harder to test outside the platform.
**Rejected.** Declarative pipelines for v1.

## ADR-007 — Silver: one table per source, conformed schema
**Decision.** `trade_flows_aeat` and `trade_flows_eurostat` share column names/types (long format) but stay separate; they meet only in Gold.

| Column | Notes |
|---|---|
| `period` (date, first day of month) | |
| `flow` (`M`/`X`) | AEAT files are wide (imports and exports side by side) → unpivot |
| `reporter_iso2` | AEAT always `ES` |
| `partner_code_src` → `partner_iso2` | mapped through `dim_country`; Eurostat uses `EL` for Greece and has confidential/aggregate partner codes that must be excluded |
| `cn8` | |
| `value_eur` (decimal) | AEAT legacy layout is in **euro cents** |
| `weight_kg` | Comext quantity unit to be confirmed (possibly 100 kg) |
| `supp_units` | |
| `source_format` | `legacy` / `2026` / `api` |
| `_source_file` / `_api_query`, `_row_hash`, `_first_loaded_at`, `_last_updated_at`, `_revision_count` | lineage and revision tracking |

**Why separate.** Spain would otherwise appear twice (double counting risk); the sources differ in scope (the 2026 AEAT format adds low-value shipments and exports declared in other member states) and revise on different cadences.
**Revision rule.** `MERGE` updates a row (and increments `_revision_count`) only when `_row_hash` of the measures changes — re-delivered identical data is not a revision.
**Bronze for the API** stores raw responses plus query parameters, so Silver can be rebuilt without calling the API again.

## ADR-008 — Gold contract and quality gates
**Gold tables.**
| Table | Grain | Purpose |
|---|---|---|
| `energy_imports_monthly` | month × product × origin | volume, value, implied price (EUR/t), share — from AEAT |
| `supply_concentration` | month × product | HHI of origins, top-1 share, number of origins — from AEAT |
| `eu_benchmark` | month × reporter × product | same metrics for ES, PT, IT, FR, DE — from Eurostat (ES included for a like-for-like comparison) |
| `revisions_summary` | reference month × run | how much official data changes after publication |

**Quality gates.**
- *Fail the run:* wrong layout (legacy line length / unexpected CSV columns), duplicate keys, null keys, negative value or weight.
- *Quarantine (not published):* implied price outside 0.1×–10× of the trailing 12-month median for the same origin and product.
- *Warn:* AEAT vs Eurostat total for Spain diverges beyond a tolerance; latest available month later than expected.

**Cut order if time runs short:** `eu_benchmark` first; `revisions_summary` last (it is the differentiator).
**Known limitations to state in the report:** EUR/t is not EUR/MWh (comparison across origins only); "country" in AEAT may be provenance rather than origin for LNG — to verify.

## ADR-009 — Visualisation and what a recruiter can actually open
**Decision.** Power BI **Desktop** in Import mode (data stored in the `.pbix`), committed with screenshots, a short GIF and a PDF export. A small Databricks AI/BI dashboard for pipeline health (freshness, quarantined rows, revision size), shown as screenshots. All business logic computed in Gold, never in DAX.
**Why.** The Power BI Service requires a work/school account (no public link from a personal account); external sharing of Databricks dashboards needs a service principal plus a custom app, and dies when Azure is switched off. The `.pbix` survives the budget.
**v1.1.** Static interactive page (e.g. GitHub Pages) reading a Gold snapshot.

## ADR-010 — Delivery in vertical slices
**Decision.** Ship a thin end-to-end slice first (crude oil only), then add revisions/2026 format, then production features. See `docs/plan.md`.
**Why.** The project only pays off once it is visible; the Azure credit window is 13 days.

---

## Open questions (to resolve in Phase 0)
- [x] AEAT max-disaggregation files: format, size, layout — see Phase 0 findings (public download confirmed).
- [x] Country for imports: **origin** (2026 "país imputado" = origin/destination; legacy has origin and provenance separately).
- [x] New 2026 format layout documented. Revisions are not flagged in the file → detected by `_row_hash`. Overlap with legacy files: to check when 2026 legacy months are loaded.
- [ ] Eurostat Comext: monthly CN8 dataset code, quantity unit, confidential/aggregate codes.
- [x] AEAT country codes: ISO alpha-2 (legacy field is 3 chars, right-padded).
- [ ] Reuse terms of AEAT data (attribution).
- [x] Trial tier not available on Free Trial subscriptions → Premium (see ADR-004 addendum).

---

## Phase 0 findings (2026-10-16, partial)
Inspected locally without loading the files into memory (streaming `awk`, ~20 s for the 2026 file).

**Legacy "Máxima desagregación" (example: `cg25en22`, January 2025).**
- Monthly data is split into **10 ZIP files by CN chapter range** (`..en22` = chapters 01–22, `..en39` = 23–39, …, `..en99` = 91–99). Chapter 27 (energy) is only in the **23–39** file (~21 MB zipped per month).
- Unzipped `cg25en22`: 155 MB, ASCII, CRLF, **fixed width, 226 characters per line** (not the 92-character layout of the monthly summaries). Field layout **not yet documented** — no record-layout file found on the AEAT pages.
- Country codes look like ISO alpha-2.

**New 2026 format (`MAXIMA_DESAGREGACION_2026.csv`).**
- One **cumulative** file (January–July 2026 at download time), 559 MB, `;`-separated, no header, CRLF, **14 columns in every row**, ~10.7 M rows (~1.5 M per month). It is republished with all months each time → full re-download + `MERGE` by design.
- Inferred columns (to confirm against `Clasi_new_fich.xlsx`): 1 flow (`E` exports / `I` imports) · 2 year · 3 month · 4 CN8 · 5 partner country (ISO2) · 6 province? · 7 net mass kg? · 8 supplementary units · 9 statistical value in **euros with decimals** (not cents) · 10 invoice currency? · 11 transport mode (1–9; `7` probably fixed installations = pipelines) · 12 `UE`/`TE` (intra/extra-EU) · 13 nature of transaction (2 digits) · 14 province/customs code?
- Imports of crude (CN 270900) Jan–Jul 2026: 301 rows; LNG (271111): 86 rows; pipeline gas (271121): 38 rows.
- **Only one partner-country column** → whether it means origin or provenance for imports is still open.

## Phase 0 findings (continued) — record layouts confirmed
Sources: `diseno226.pdf` (legacy layout) and sheet *Diseño de registro* in `Clasi_new_fich.xlsx` (2026 layout).

**Legacy (226 chars) — declaration-level records, not aggregates.** Key positions: 1 flow · 2–3 year · 4–5 month · 6–7 customs province · 20–25 document admission date · 26–37 statistical position (first 8 = CN8) · 38 declaration type (I=Intrastat, D=customs declaration) · **67–69 country of origin/destination** · **70–72 country of provenance/consignment** · 76–77 province of origin/destination · 90–104 weight · 105–119 units · 120–131 statistical value · 132–143 invoice value · 165 transport mode at border · 171 zone (C=EU, T=third countries) · 173–174 nature of transaction · 225–226 fiscal-domicile province.

**2026 CSV — aggregated records (14 columns):** flow · year · month · CN · **imputed country (= origin for imports)** · province of origin/destination · weight · units · statistical value · currency · transport mode at border · zone (UE/TE) · nature of transaction · fiscal-domicile province.

**Unit trap (verified on crude oil, CN 2709, imports):**
| | Weight | Value | Check |
|---|---|---|---|
| Legacy (Jan 2025) | 3 implied decimals → divide by 1,000 to get kg | **euro cents** → divide by 100 | 83 declaration rows ≈ 5.1 Mt and ≈ €2.7 bn → ≈ €530/t, plausible |
| 2026 CSV (Jan 2026) | kg | euros with decimals | 35 aggregated rows ≈ 4.6 Mt and ≈ €1.9 bn → ≈ €406/t, plausible |

**Transport mode code list confirmed:** 7 = fixed transport installations (pipelines). Pipeline gas (271121) Jan 2025: 8 of 9 rows mode 7, one row mode 8 (inland waterway) → candidate data-quality warning.

**Grain mismatch:** legacy rows are individual declarations (with admission date); 2026 rows are aggregates. Harmonisation requires aggregating legacy to a common grain.

**Volume:** the chapter 23–39 legacy file is ~170 MB unzipped per month (~749k rows) → ~14 GB of raw text for 2019–2025.

## ADR-011 — Silver scope: CN chapter 27 only
**Decision.** Bronze keeps the AEAT "23–39" files (legacy) and the full 2026 CSV as delivered; Silver keeps **all of chapter 27** (crude, gas, refined products, coal, electricity).
**Why.** Ingesting every chapter would be >100 GB unzipped for 2019–2026 — outside the time and budget window. Chapter 27 still gives realistic volume and already covers the future "fuel prices" domain.
**Rejected.** All products in Silver (cost); only the three CN codes of v1 (blocks expansion).

## ADR-012 — Silver grain
**Decision.** One Silver row per **flow × period × CN8 × country of origin/destination × transport mode at border**, with weight (kg), value (EUR) and supplementary units summed. This is the `MERGE` key.
**Kept in Bronze only:** provinces, nature of transaction, currency, fiscal-domicile province, and — from the legacy files — document admission date and country of provenance (no 2026 equivalent).
**Why.** A lean, always-populated key keeps `MERGE` and revision tracking reliable (province is sometimes empty in the 2026 file); none of the dropped dimensions answers a v1 question.
**Rejected.** Full 2026 grain (fragile key); keeping province (interesting Galician angle — A Coruña / Mugardos LNG terminal — deferred after v1).
**Test fixtures.** `tests/fixtures/` holds a few real chapter-27 lines from each format for unit tests of parsing and unit conversion.

## ADR-004 addendum (2026-10-16) — Workspace tier
The Databricks **Trial** pricing tier is rejected for this subscription type (`TrialSkuNotSupportedForSubscriptionType`: not supported on Azure Free Trial / Azure for Students). Workspace created as **Premium** (required for Unity Catalog); DBUs are paid from the Azure credit. Free Trial subscriptions also have a low regional vCPU quota → single-node clusters only; check *Usage + quotas* before creating compute.

## ADR-013 — Workspace type and compute under a 4-vCPU quota (2026-10-17)
**Decision.** Azure Databricks **Hybrid** workspace (classic + serverless), **Premium** tier, **Secure Cluster Connectivity ("No Public IP") disabled** at creation. Classic compute = **one single-node cluster with 4 vCPUs** (the subscription's total regional vCPU quota is 4).
**Why.**
- Hybrid keeps the customer-owned ADLS Gen2 + Access Connector + Unity Catalog external-location pattern (the realistic Azure enterprise setup) and still allows serverless where it pays off.
- With SCC enabled on the default managed VNet, Azure creates a NAT gateway automatically, which bills hourly even when idle — incompatible with the USD 20/month ceiling.
- 4 vCPUs allow exactly one 4-vCPU node at a time: an interactive cluster and a job cluster cannot run together. Scheduled jobs therefore run on **serverless jobs compute** (does not consume the subscription's VM quota) or only when the interactive cluster is terminated. The Power BI connection uses a serverless SQL warehouse (also outside the VM quota).
**Rejected.** Serverless-only workspace (default storage instead of own ADLS; unclear support for own external locations); SCC enabled (NAT gateway cost).

---

## Phase 1 findings (2026-10-20) — Silver parsers validated on the full sample files
Parsers in `src/es_energy_trade/transforms/aeat.py`, unit-tested in `tests/test_aeat_transforms.py` against sums computed independently with `awk`; then run on the full local samples (Spark local mode).

- **Flow letters:** `I` = import, `E` = export in both formats (Silver `flow`: `M` / `X`).
- **Reconciliation with Phase 0 figures** (crude oil, CN 2709, imports): legacy Jan 2025 → 5.14 Mt, EUR 2.71 bn (~EUR 527/t); 2026 CSV Jan 2026 → 4.64 Mt, EUR 1.88 bn (EUR 406/t). Same totals as the Phase 0 `awk` checks.
- **Supplementary units in the legacy layout also have 3 implied decimals.** Not documented in `diseno226.pdf`; inferred because tonnes-per-unit is ~18 for LNG in both formats only if legacy units are divided by 1,000 (e.g. Russia LNG: 18.1 legacy vs 18.2 in 2026). The unit itself (what one "unit" is) is still unidentified; supplementary units are not used in Gold v1.
- **2026 file contains chapter-27 rows with an empty transport mode** (26 rows over Jan–Jul 2026): residual CN `27000000` (low-value shipments, no province/mode/nature) and a few exports to UZ. The legacy sample has none.
- **CN length:** the 2026 file mixes 8- and 10-digit CN codes within the same month; Silver truncates to CN8.
- **Gold must filter by product prefix** (2709, 271111, 271121). Residual `27000000` rows stay in Silver (ADR-011) but are not a product.

## ADR-014 — Missing transport mode is a sentinel, not a null key (2026-10-20)
**Decision.** When the transport mode at the border is empty, Silver stores `transport_mode = 'NA'`. All other key columns still fail the run if null.
**Why.** ADR-012 assumed an always-populated key; the 2026 file proves transport mode is not. A null in a `MERGE` key never matches, so those rows would be re-inserted on every run. `NA` keeps them stable and countable (26 rows in Jan–Jul 2026, none affecting crude, LNG or pipeline gas imports).
**Rejected.** Failing the run on these rows (the file is legitimately like that); dropping them (loses residual volume silently); removing transport mode from the key (breaks the LNG vs pipeline split, which depends on it).
**Amends** ADR-012 (key unchanged; only the null handling is added).

## Phase 1 findings (2026-10-20, continued) — what the AEAT site actually looks like
Checked by reading the year and month pages for 2019, 2024, 2025 and 2026 (page text only; raw HTML could not be fetched from the workspace tooling).

- **Link discovery is mandatory, not a nice-to-have.** The month code in the file name is irregular (`en`, `fb`, `sp`, `dc` for Jan, Feb, Sep, Dec; the rest not verified), and the ZIP base path differs between 2019 (`.../Contenidos_Privados/.../datos_mensuales_maxima_desagregacion/`) and 2024–2026 (`.../Tema/Aduanas/Comercio_exterior/maxima_desag_mens/`). The switch year is unknown. Month sub-page URLs also differ (`{year}-maxima-desagregacion/{mes}.html` vs `{year}/{mes}_.html`), and the month shown on the year page differs (December for 2019, January for 2024–2026). This corrects the assumptions in ADR-003 and `config/sources.yml`.
- **The 2026 months are also published as legacy ZIPs.** The 2026 year page lists `cg26en39.zip` etc. for January–July 2026 (same name pattern, ~21 MB zipped for chapters 23–39). The cumulative CSV used in Phase 0 is not linked from that page, so its source URL is **unknown**. Consequence: 2026 months exist in two formats, which reopens the "overlap with legacy files" item and the double-counting risk of ADR-007. **Open: which source is canonical for 2026 months.** Not yet verified that the 2026 legacy ZIPs have the same 226-character layout.
- **Possible layout change after September 2025.** The chapter 23–39 ZIP grows from ~13 MB (Jan 2025) to ~24.6 MB (Sep 2025) and ~21.7 MB (Dec 2025), while 91–99 shrinks from ~55 MB to ~10 MB. The Sep, Oct and Dec 2025 pages link a PDF "Nota importante sobre los datos de septiembre" (not read). Phase 0 only inspected January 2025. The legacy reader fails the run on any line that is not 226 characters, so this will surface at load time; read the note before the backfill.

## ADR-015 — Source of truth by period: legacy ZIPs to Dec 2025, cumulative CSV from Jan 2026 (2026-10-20)
**Decision.** The 2026 cumulative CSV ("nuevo formato 2026") is the source of truth for every 2026 month. History comes from the monthly legacy ZIPs, **2019-01 to 2025-12**. The legacy ZIPs that the site also publishes for Jan–Jul 2026 are **not loaded**. The CSV is re-downloaded on each release and each real change is kept as a dated Bronze snapshot; Silver `MERGE` + `_row_hash` detects the revisions.
**Why.** The CSV is the format that carries continuous revisions of past months, which is what the project must demonstrate (ADR-001, ADR-007). Loading both formats for the same months would double count.
**Consequences / known limitations.**
- Structural break at January 2026: legacy rows are declarations, CSV rows are aggregates and the CSV adds low-value shipments (residual CN `27000000`) and other scope differences (ADR-007). A like-for-like check is possible because `cg26en39.zip` (Jan 2026, legacy) exists: compare crude, LNG and pipeline-gas imports of Jan 2026 in both formats and state the difference in the report. **Pending, not yet done.**
- The CSV URL contains the last covered month (`.../julio/MAXIMA_DESAGREGACION_2026.csv`), so it changes on every release; it is read from the 2026 page, never hard-coded. At the time of writing (page updated 2026-09-22) it covers January–July 2026; August onward is not published in either format.
**Rejected.** Legacy ZIPs for 2026 (no revision story); loading both and reconciling in Silver (double-counting risk for no gain in v1).

## Environment — Bronze volume (2026-10-20)
Raw files live in the Unity Catalog volume `portfolio.bronze.raw_files` (`/Volumes/portfolio/bronze/raw_files`): `aeat/legacy/{yyyy-mm}/` (ZIP kept as delivered + unpacked file) and `aeat/csv_2026/snapshot={date}/`.
