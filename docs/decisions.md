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

## Phase 1 findings (2026-10-23) — the AEAT links are irregular inside a single month
Found when the first Bronze run failed on `.../2022/cg22jl61.zip`. On the July 2022 page, nine of the ten ZIPs sit under `.../2022/Julio/` (capitalised folder) but the 54–61 file sits directly under `.../2022/`, with no month folder. The link text is regular (`Julio 2022 54 al 61(...)`), so the month is now read from the link text first, then the URL folder, then the sub-page being read; conflicting sources are not trusted. An unplaceable link is dropped unless it is the chapter 23–39 file, in which case the run stops. Other irregularities in the 84 months 2019–2025 are possible; they surface as an error naming the URL.

## ADR-015 addendum (2026-10-28) — how CSV releases are identified, stored and checked
**What happened.** Between two checks the 2026 page moved from the July release (`.../julio/MAXIMA_DESAGREGACION_2026.csv`) to the August release (`.../agosto/MAXIMA_DESAG_AGOSTO26.csv`): new folder **and new file name**. A discovery keyed on the file name broke.
**Decision.**
- *Identification:* the stable part of the URL is the folder `Nuevo_formato_2026_max_des/<month>/`, which also gives the last covered month. If the file name carries a month, it must agree with the folder. If the page keeps several releases, the latest month wins; two different files for the latest month stop the run.
- *Storage:* every real change is a `snapshot=YYYY-MM-DD[-n]/` folder (never overwritten) with a `manifest.json` (URL, last covered month, size, **SHA-256**, ETag, Last-Modified, download time, origin). The on-disk name is fixed (`MAXIMA_DESAGREGACION_2026.csv`) whatever the release called it. A download is skipped only when ETag or Last-Modified prove it is the same file; byte-identical content under a new URL is discarded; size alone is never proof. Transient network failures are retried; a truncated file is deleted. A snapshot placed by hand gets its manifest from its content.
- *Silver gates for the CSV:* no period before 2026-01 (the legacy ZIPs own 2019–2025); every month from 2026-01 to the release's last month must be present (this is what proves the file is cumulative, instead of assuming it) and the last month must match what the URL claims.
**Open until checked on the data:** whether the August file really is cumulative (Jan–Aug). The coverage gate will say.

## Phase 1 findings (2026-10-29) — format break measured on January 2026 (the check ADR-015 left pending)
January 2026 exists in both formats: legacy ZIP `cg26en39` and the cumulative CSV (July vintage, Jan–Jul). Both were run through the same parsers (`parse_legacy` / `parse_2026` → `to_silver` → `energy_imports_monthly`) and compared, imports only. Script and inputs are local (`data/samples/`, not committed).

| Product | Legacy ZIP | CSV | Legacy / CSV (weight) |
|---|---|---|---|
| Crude oil (2709) | 4,669.4 kt · EUR 1,874.9 M | 4,639.8 kt · EUR 1,884.0 M | 1.006 |
| LNG (271111) | 1,440.5 kt · EUR 579.4 M | 1,422.1 kt · EUR 587.1 M | 1.013 |
| Pipeline gas (271121) | 465.3 kt · EUR 172.3 M | 532.0 kt · EUR 198.0 M | 0.875 |

- **Crude and LNG are continuous across the break** (within about 1% in weight; same origins in both). The January 2026 LNG concentration spike (US ≈ 71% of weight) is present in the legacy file too, so it is not a CSV artefact.
- **Pipeline gas is not**: the CSV is 14.5% higher, entirely from Algeria (legacy 459.6 kt vs CSV 526.3 kt). Cause unknown: wider scope of the new format, or a revision between the legacy publication and the CSV vintage; this comparison cannot tell them apart. Treat the pipeline-gas level before and after January 2026 as not strictly comparable.
- Crude from Italy differs (legacy 125.5 kt, CSV 98.2 kt); other crude origins are identical to the tonne.
- The residual partner codes are not stable between formats (`QU` in legacy, `QW` in the CSV for the same tiny pipeline flow); small, but a phantom origin change at the break is possible.
- **Exact duplicate lines are normal in the legacy files** (6.1% of chapter-27 lines in Jan 2025, 7.3% in Jan 2026): identical declarations. They are summed, not removed; removing them would break the agreement with the CSV above.
- **The doubling of the ZIP size from September 2025 is not chapter 27**: chapter-27 lines grew from 21,849 (Jan 2025) to 26,515 (Jan 2026), +21%, while the whole 23–39 file doubled (749k to 1.45M lines).
- **September 2025 note (AEAT):** the files published on 17 November 2025 were incomplete because of an error generating them; users who downloaded before 5 December 2025 must download again. The Bronze files were downloaded in October 2026, so they should be the corrected version (not proven from the files). In the Gold, September 2025 crude and LNG look normal but pipeline gas is 220 kt against 430–590 kt in neighbouring months. **Unexplained; do not state a cause.**
- **Implied price per origin is not usable for small flows** (e.g. a UK crude row of 11.8 kg at EUR 17,905/t). The quarantine of ADR-008 is not implemented; until it is, show price only at product level or above a minimum volume.

## ADR-016 — Implied price is withheld below 1,000 t per origin-month (2026-10-29)
**Context.** `implied_price_eur_per_t = value / weight` is only meaningful when the flow is large. Measured on the Silver export (2019-01..2026-08, imports only), the maximum origin-month price before any rule was 8.5 million EUR/t (crude oil), 2.8 million EUR/t (LNG) and 12.35 billion EUR/t (pipeline gas). A Power BI average or max over that column would be dominated by a handful of near-zero-weight rows. ADR-008 planned a quarantine for these rows; that was never implemented.

**Decision.** `energy_imports_monthly` sets `implied_price_eur_per_t` to NULL when the origin-month weight is below `MIN_WEIGHT_FOR_PRICE_KG` = 1,000,000 kg (1,000 t; parameter `min_weight_for_price_kg`). The rows stay: weight, value, `share_of_weight`, HHI and `n_origins` are unchanged. The rule lives in Gold (`gold.py`), not in DAX.

**Effect, measured.** 348 of 2,937 origin-months lose their price. They carry 0.0008 % of crude-oil weight, 0.0076 % of LNG and 0.0377 % of pipeline gas. Maximum price afterwards: 2,106 (crude), 3,593 (LNG), 1,689 (pipeline gas) EUR/t.

**What the rule does not do.**
- 1,000 t is a judgment, not a derived value. Raising it to 10,000 t would drop another 32 crude, 17 LNG and 86 pipeline-gas origin-months and barely move the crude/LNG maxima, so the choice is not sensitive for those two; it is for pipeline gas, where a few origins carry most of the volume.
- It is deliberately not a price band. The low prices left in the data are real (crude 68.8 EUR/t for Italy in May 2020; the April–May 2020 oil crash) and so are the high LNG prices of 2022 (up to 3,593 EUR/t). A fixed band would delete exactly the periods that make the series interesting.
- Some small crude flows still show 1,000–2,100 EUR/t (FR, NL, DK, MT; 3–35 kt, 2022–2024). The cause is not established and no explanation is stated.

## Findings (2026-10-29, continued) — Silver export, revisions and September 2025
- **Reproducibility.** Re-running the repository's `energy_imports_monthly` on the exported Silver (141,878 rows: 127,512 legacy 2019-01..2025-12, 14,366 from the 2026 CSV 2026-01..2026-08) reproduces the Gold table exported from Databricks: 2,937 rows, 0 differences (before ADR-016).
- **Revisions (July vs August 2026 vintage of the CSV).** 195 rows changed, all in periods 2026-01..2026-07: 122 imports, 73 exports. Rows per month: Jan 10, Feb 16, Mar 21, Apr 16, May 25, Jun 33, Jul 74. Sum of absolute value changes: about 649 M EUR (imports), 101 M EUR (exports). Median relative change of value is 2.6 %, the 90th percentile 91 %, the 99th percentile 3,193 % (very small rows). Revisions are therefore concentrated in the most recent months but also reach January.
- **Project products.** Examples in imports: crude Mexico June 403 → 590 kt (+46 %), Mexico July 561 → 706 kt, US July 813 → 907 kt, Algeria July 487 → 395 kt (−92 kt); LNG US March 764 → 831 kt. LNG Mauritania and Senegal in March each lose almost exactly the same weight (about 8.02 kt, with the value falling by about 5.8 M EUR in each); the reason is not known. Two project-product rows (June, NG and RU LNG) changed with identical weight and value; `_row_hash` also covers supplementary units, which was not inspected.
- **Consequence.** A dashboard built on the latest CSV is not final for at least the previous six months. The README must say so; `_revision_count` and the revisions table are the evidence.
- **September 2025, pipeline gas.** 220 kt against 520 kt (Aug) and 428 kt (Oct). Algeria is 219 kt (508 / 424 around it); the rows coded NO and QU present in August are absent. Total chapter-27 import rows that month are normal (754 vs 638–742 around it), so a missing file is not the explanation. Value falls with weight (about 374 EUR/t against 412 and 386), so it does not look like a unit error either. A partial omission or a real drop cannot be told apart with this data alone. Pedro compared September 2025 against another source (not recorded which) and the 220 kt matches, so this is not a Bronze or parsing defect. Whether that source is independent of the AEAT declarations was not checked, and the reason for the drop itself is not established.

## ADR-015 addendum (2026-10-29) — keys that vanish between CSV releases are not removed (defect, open)
**Observation.** The `MERGE` in `silver_merge.py` has no `WHEN NOT MATCHED BY SOURCE` branch, so a key present in the July release and absent from the August release stays in Silver. ADR-015 says the CSV is the source of truth for 2026; that is not what the table does in this case.

**Size, from the run outputs and the export (no access to the August file here).**
- Silver 2026 rows after the August run: 14,366. August batch rows (gate output): 14,357, with 0 duplicate keys. Difference: 9 Silver rows that the latest release does not contain.
- Consistent with the merge result: 12,522 rows still sourced from the July snapshot, 195 updated, 1,649 inserted = 1,628 rows of period 2026-08 plus 21 new keys in earlier periods. July batch = 12,522 + 195 = 12,717; 12,717 + 21 − 9 = 12,729 = August keys in periods up to July.
- What the 9 rows are is not known (a re-keyed row, a removed row, a changed partner or transport code?). The 21 new keys in earlier periods may be the other half of the same effect; that is a hypothesis.

**Identified (2026-10-29, against the August file, SHA-256 f7482742…, same as the manifest).** Re-parsing the August CSV with the repository code gives 14,357 rows; all 14,357 keys exist in Silver with identical weight and value (0 differences). The 9 extra Silver rows are:
- imports, May 2026: LNG Mauritania 32.0 kt (EUR 22.6 M) and LNG Senegal 34.0 kt (EUR 20.1 M), CN 27111291 from the US 28.0 kt (EUR 18.5 M), CN 27129011 China 13.8 t; July 2026: CN 27079999 Azerbaijan 1.8 kt, CN 27101211 Egypt 32.5 kt. Imports total 128.4 kt, EUR 81.2 M.
- exports: May CN 27132000 Ireland 7.4 kt, June CN 27112900 France 2.6 kt, July CN 27111100 partner QW 3.3 kt. Total 13.3 kt, EUR 6.2 M.
- The 21 keys inserted in earlier periods by the August run are almost all exports (Netherlands LNG, UK, Algeria, US, Turkey…) and do not pair with the 9 vanished keys by period, product and partner. The "re-keyed rows" hypothesis is not supported; why AEAT dropped them is unknown.
- Gold effect: only LNG May 2026 changes. Total 1,216 kt → 1,150 kt (−5.4 %), origins 8 → 6, HHI 2,692 → 2,993, top-1 share 41.1 % → 43.4 %. Mauritania and Senegal also lost about 8 kt each in March (see revisions above), so both flows moved between the July and August releases.

## ADR-017 — Keys missing from the latest CSV release are deleted, after being logged (2026-10-29)
**Decision.** For `source = csv2026` the MERGE gets `WHEN NOT MATCHED BY SOURCE AND t.source_format = '2026' THEN DELETE`, which makes the CSV the source of truth as ADR-015 states. Before the MERGE, `vanished_rows` writes the keys about to disappear to `trade_flows_aeat_revisions` (`_change_type = 'vanished'`, `*_new` columns NULL; `changed_rows` now writes `'revised'`; the 195 rows written before this change have `_change_type` NULL and are revisions). Never used for legacy loads (a legacy batch covers a few months, not the whole format).
**Guard.** `check_vanished_share` fails the run if more than 1 % of the existing 2026 rows would vanish (a truncated file would otherwise wipe Silver). The August case is 9 of 14,366 rows (0.06 %).
**Verification, honestly.** `vanished_rows`, the gate and the generated SQL are unit-tested locally (56 tests). The MERGE with `NOT MATCHED BY SOURCE` was NOT executed on Delta: the Databricks credits expired before the change was made. The syntax is Delta SQL (DBR 12.2+); it is untested here.
**What the offline data is.** `data/silver_offline/trade_flows_aeat.csv` = legacy rows from the Databricks export + 2026 rows rebuilt from the August file by the same functions. `data/gold_offline/*` is built from it with ADR-016. The Delta tables in the (expiring) workspace still contain the 9 rows and no ADR-016 rule; they are not what the report shows.
