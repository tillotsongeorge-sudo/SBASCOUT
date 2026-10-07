# SCOUT.md: Off-market business scout runbook

**Target:** Texas plumbing / HVAC contractors (NAICS 238220) that received PPP loans with
`CurrentApprovalAmount >= $150,000`, cross-checked against Texas registration records and
DOL retirement-plan filings.

This runbook is portable. It needs only **Python 3.9+ (standard library only)**, outbound HTTPS,
about 2.5 GB of free disk space, and about 1 GB of RAM. It does not depend on any agent "skill",
IDE, or hosted service. Any shell that can run Python and reach the four public sources below can
run it.

```bash
python3 scout.py all          # fetch (reuses cached files) -> build -> verify
python3 scout.py fetch --force  # re-download everything even if unchanged
python3 scout.py build        # rebuild from data/raw without network
python3 scout.py verify       # integrity checks only
```

The settings live in `scout_config.json`: state, NAICS code, amount threshold, age threshold, ID
format, and source URLs.

## 0. Capability check (do this first)

The scout needs four capabilities. If any of them fails, stop and connect an execution environment
that provides it, such as a Cursor Cloud Agent, a local terminal, CI, or any VM with Python and
network access. A chat-only assistant without code execution **cannot** run this scout. It can only
describe it.

| Capability | Test |
|---|---|
| Run code | `python3 -c "print('ok')"` |
| Download full files | `curl -sSI https://data.sba.gov/sites/default/files/distribution/SBA-OCA-2022-07-001/public_150k_plus_240930.csv` returns 200 with a ~452 MB length |
| Call APIs | `curl -s "https://data.texas.gov/resource/9cir-efmm.json?\$limit=1"` returns JSON |
| Save outputs | `touch data/.w && rm data/.w` |

## 1. Sources

| source_id | What | URL |
|---|---|---|
| `SBA_PPP_150K_PLUS` | SBA PPP FOIA, all loans of $150K and above (one CSV). The newest `public_150k_plus_YYMMDD.csv` is discovered from the dataset page. | https://data.sba.gov/dataset/ppp-foia |
| `TX_FRANCHISE_9CIR_EFMM` | Texas Comptroller **Active Franchise Taxpayers** (full CSV export, about 3.5M rows) | https://data.texas.gov/dataset/Active-Franchise-Taxpayers/9cir-efmm |
| `DOL_FORM_5500_<year>` / `DOL_FORM_5500_SF_<year>` | EBSA Form 5500 and 5500-SF "Latest" datasets, **national** (all sponsor states). Uses the two newest plan years listed on the DOL page. | https://www.dol.gov/agencies/ebsa/about-ebsa/our-activities/public-disclosure/foia/form-5500-datasets |

Raw files are streamed to `data/raw/` while being SHA-256 hashed. They are kept on disk but are
git-ignored because of their size. Every fetch appends to `data/download_log.csv` with the URL,
final URL, UTC retrieval time, HTTP status, ETag, Last-Modified, bytes, SHA-256, and the action
taken (`downloaded`, `cached`, or `failed`). That log, together with the URL and hash, makes the
input reproducible. The matching SBA rows are also saved verbatim as UTF-8 in
`data/extracts/sba_ppp_tx_238220_150k_raw_rows.csv`.

**Encoding:** each line is decoded as strict UTF-8 first, then cp1252, then latin-1. The counts
from each step go into `data/source_manifest.json`. The SBA file decodes as **cp1252**.

## 2. Pipeline stages

1. **SBA filter (streamed).** Every row is read, without loading the file into memory. A row is kept
   when `BorrowerState == "TX"`, `NAICSCode == "238220"`, and `CurrentApprovalAmount >= 150000`.
   The pipeline records source data rows, physical lines, malformed rows, TX rows, TX+NAICS rows,
   rows below the threshold, and duplicate `LoanNumber`s. Rows with a blank BorrowerState but
   ProjectState=TX are counted and listed but **excluded**, because the rule is BorrowerState.
2. **Loans.** Each `LoanNumber` is stored once in `data/loans.csv`. First draws (`PPP`) and second
   draws (`PPS`) are separate rows and are never merged.
3. **Business grouping.** Loans are linked with union-find when any of these holds:
   - the same normalized legal name (entity suffix ignored) **and** the same normalized street + ZIP;
   - the same normalized legal name **and** the same ZIP (the street spelling differs);
   - a **documented DBA**: one loan's BorrowerName contains `DBA` / `D/B/A` / `AKA`, the DBA text
     equals another loan's legal name, and both share a street + ZIP.

   Loans at the same address with different names and no DBA evidence stay separate. They are written
   to `data/review_queue.csv` (stage `business_grouping`).
4. **Permanent IDs** (`biz-001`, `biz-002`, ...). `data/business_registry.csv` maps each ID to every
   identity key (`core name|street|ZIP`) and loan number it has ever had. On a rerun, an ID is
   reused when any loan number or identity key matches. Otherwise a new ID gets `max + 1`. IDs are
   never reused or renumbered. If a rerun merges two IDs, the lower one survives, the other is
   marked `merged` in the registry, and non-pipeline tracker columns are carried over. Businesses
   that vanish from the source stay in the tracker with `in_current_source=no`.
5. **Texas registration (9cir-efmm).** Candidates come from an equal normalized name (legal name or
   documented DBA) or an equal street+ZIP with a similar name.
   - `high`: the legal name matches (entity suffix ignored) **and** the street address (or house
     number + street) and ZIP match
   - `medium`, any one of:
     - the exact legal name *including the suffix* matches, and the ZIP matches
     - the exact legal name matches, the city matches, and it is the only active Texas record with
       that name statewide
     - a documented DBA matches, and the street matches
     - a DOL filing ties a name-only candidate to the PPP borrower. The sponsor EIN must equal the
       FEIN inside the Texas taxpayer number, the sponsor name must equal the Texas name, and the
       sponsor address must equal the PPP address.
   - `low`: anything else. This includes a name match without corroboration, or the same address
     with only a similar name. These go to review.
   - Exactly one top `high`/`medium` candidate gives `verified`. Among tied candidates, the one
     whose exact suffix matches wins. Remaining ties go to `review`.
   - If the verified record's entity type differs from the PPP borrower's, such as PPP `INC` vs
     Texas `LLC`, it is flagged `suffix_conflict` and goes to review. A conversion or successor
     entity can carry a younger charter date than the business itself.

   **Record date** is the `SOS Charter Date`. Its meaning depends on the record type and is saved per
   business:
   - `U`: Texas formation date
   - `V`: Certificate of Authority date, when a foreign entity registered in Texas. It formed
     elsewhere earlier or on the same day.
   - `X`: no charter date, so the date is unknown

   Original trade-license dates (TDLR / TSBPE) are **not available** in bulk (see section 5), so they
   are never used. `record_age_30plus=yes` when the record date is 30 or more years before the run
   date.
6. **Pension (DOL 5500 + 5500-SF, national).** Sponsors are matched by:
   - an **independently known EIN**: a Texas taxpayer number of the form `1` + 9-digit FEIN + check
     digit, taken from the verified Texas record. Comptroller-assigned `3...` numbers carry no EIN.
   - or the sponsor name / DBA name equal to the business legal name or DBA.

   Confidence: `high` when the EIN and name agree, or the name and street agree. `medium` when the
   name and ZIP agree, or the EIN and address agree. `low` goes to review and is never used. An
   uncertain pension join only puts that plan in review. It never changes `eligibility_status`,
   because pensions are informational, not an eligibility test.

   Rows must have a retirement benefit code, meaning a `TYPE_PENSION_BNFT_CODE` code starting with
   `1` (defined benefit) or `2` (defined contribution). Welfare-only plans and DFE filings are
   dropped. For each plan ID (`EIN-PN`), only the newest plan year / filing is kept. The tracker
   saves the plan ID, actual plan period (begin..end), `DATE_RECEIVED`, and end-of-year active
   participants **per plan**. The participant counts are **never summed** because plans overlap. The
   tracker shows only the largest single plan. A missing filing becomes
   `pension_status=unknown (...)`, which **does not exclude** a business.
7. **Payroll proxy.** `historical_payroll_proxy_annual = loan / 2.5 * 12` is computed only when all
   of these hold. Otherwise it is `unknown`.
   - The loan is a first or second draw (PPP/PPS) under NAICS 238220, not NAICS 72 (which uses 3.5x).
   - The business type is a Corporation or S-Corp. LLCs, partnerships, and sole proprietors can use
     owner self-employment income.
   - There is no EIDL refinance.
   - The current amount equals the initial approval amount.
   - The amount is below the program cap ($10M first draw, $2M second draw).
   - JobsReported is present, and the implied pay per job is below the $100K cap.

   The first draw is preferred. Loans are never summed. `JobsReported` is kept separately per loan
   as historical data. **Revenue and purchase price are never inferred.**
8. **Eligibility.**
   - `ready`: a verified Texas join with a record dated 30+ years ago
   - `closed`: a verified Texas join with a record under 30 years, the clear exclusion with current
     evidence
   - `review`: everything uncertain, including ambiguous joins, name-only matches, entity-type
     conflicts, no Texas match (for example sole proprietors, who don't file franchise tax), or no
     charter date
9. **Outreach.** `outreach_status` is set **only when an ID is created**: `ready` if the business is
   eligibility-ready, `not_ready` otherwise. On every rerun, outreach_status, scores, notes, and any
   column this pipeline does not own are copied back unchanged.

## 3. Column ownership

This pipeline ("Prompt 1") owns and rewrites only the identity, loan, registration, pension, and
eligibility columns listed in `OWNED_COLUMNS` in `scout.py`. Every other column in
`scout-tracker.csv` is preserved on reruns, whether it is `outreach_status` or one a person or
another prompt added (score, owner, notes, and so on). Add new columns freely. Don't rename the
owned ones.

## 4. Outputs

| File | Contents |
|---|---|
| `scout-tracker.csv` | One row per business (`biz_id`) |
| `results.html` | Readable page with stage counts, five ready businesses with clickable evidence, unknowns, coverage, and unavailable sources (`index.html` redirects to it) |
| `data/loans.csv` | One row per PPP loan/draw |
| `data/source_ledger.csv` | One row per (biz_id, source_id, source_record_id) with URL, retrieval date, source SHA-256, reporting period, match method, evidence, confidence, and first/last run |
| `data/tx_franchise_candidates.csv` | Every Texas candidate considered, with confidence and evidence |
| `data/pension_plans.csv` | Matched plans (latest filing per plan) plus low-confidence candidates marked for review |
| `data/review_queue.csv` | Uncertain joins by stage |
| `data/business_registry.csv` | The permanent ID registry |
| `data/download_log.csv`, `data/source_manifest.json` | Hashes, encodings, and full row counts |
| `data/coverage_report.json` | Stage counts for the latest run |
| `data/run_history.csv` | One line per build |
| `data/unavailable_sources.csv` | Each unavailable source, logged **once** |
| `data/verification.json` | Results of the `verify` checks |

## 5. Known limits

- **Trade-license original dates are unavailable.** TDLR's open dataset (`7358-krk7`) has only
  expiration dates, and TSBPE has no bulk dataset. SOS filing history (SOSDirect) is paid and
  login-only, so a conversion or merger that reset a charter date is not detected. A `closed`
  business may be older than its current charter.
- **The Texas active franchise list holds active taxpayers only.** A business missing from it
  becomes `review`, not `closed`.
- **The Texas taxpayer address is often a mailing or accountant address.** Name-only matches
  therefore land in review. Clear them manually from `data/review_queue.csv` by checking the
  linked record.
- **The newest DOL plan year is partial** because filings lag up to about 9.5 months after year end.
  The previous year is searched too, and the newest filing per plan is kept.
- **None of these sources says anything about owner age, retirement, or intent to sell.**

## 6. Rerun checklist

1. `python3 scout.py all`, then confirm `data/verification.json` shows `"passed": true`.
2. Check `data/run_history.csv`. When the source hash is unchanged, `new_ids` should be `0`.
3. Check `git diff scout-tracker.csv`. Only `last_seen_run` should change for unchanged sources.
