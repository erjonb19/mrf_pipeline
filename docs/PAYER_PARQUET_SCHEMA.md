# `payer_parquet/` output contract

What `run_pipeline.py` writes, and what a downstream consumer may rely on.

Profiled 2026-09-06 with DuckDB 1.5.4 against all 18 completed files in
`payer_parquet/`. Every count below is measured, not estimated.

---

## 1. Shape

One Parquet file per entry in `config.PAYER_FILES`, named `<payer>.parquet`,
written by pyarrow (SNAPPY, 200k-row row groups). A file appears only when its
parse finished: the writer builds `<payer>.parquet.part` and renames on
success, so **the presence of a `.parquet` file is the completion signal**.

All 18 files share one identical schema — 18 columns, same names, same types,
same order. There is no partitioning and no manifest; the glob
`payer_parquet/*.parquet` is the rate dataset.

**The run is complete.** All 18 configured payers parsed: 7 Aetna, 5 Cigna,
6 UnitedHealthcare. **54,636,339 rows.**

| payer | rows | `network_names` | `last_updated_on` | schema |
|---|---:|---|---|---|
| `AetnaALIC_Epo` | 5,000,723 | *(empty)* | 2026-08-05 | 2.0.0 |
| `AetnaALIC_Hmo` | 170,963 | `Aetna Choice POS II` | 2026-08-05 | 2.0.0 |
| `AetnaALIC_OpenAccessElectChoice` | 5,925,265 | *(empty)* | 2026-08-05 | 2.0.0 |
| `AetnaALIC_OpenAccessHealthNetworkOption` | 59,248 | *(empty)* | 2026-08-05 | 2.0.0 |
| `AetnaALIC_OpenAccessManagedChoice` | 6,591,660 | *(empty)* | 2026-08-05 | 2.0.0 |
| `AetnaALIC_Ppo` | 4,659,309 | *(empty)* | 2026-08-05 | 2.0.0 |
| `Aetna_NY` | 1,324,087 | *(empty)* | **2026-06-05** | 2.0.0 |
| `Cigna_LocalPlus` | 1,025,990 | `LOCALPLUS WITH EBH PLUS PATHWELL` | 2026-08-01 | 2.0.1 |
| `Cigna_NationalOAP` | 1,323,489 | `NATIONAL OAP` | 2026-08-01 | 2.0.1 |
| `Cigna_NationalPPO` | 1,393,531 | `NATIONAL PPO` | 2026-08-01 | 2.0.1 |
| `Cigna_PathwellOAP` | 1,323,489 | `PATHWELL OAP` | 2026-08-01 | 2.0.1 |
| `Cigna_PathwellPPO` | 1,393,531 | `PATHWELL PPO` | 2026-08-01 | 2.0.1 |
| `UHC_NY_ChoiceEPO` | 3,006,815 | `Choice-EPO` | 2026-08-01 | 2.0.0 |
| `UHC_NY_ChoiceEPO50` | 5,044,110 | `EP1 50` | 2026-08-01 | 2.0.0 |
| `UHC_NY_ChoicePlus` | 3,006,815 | `Choice-Plus` | 2026-08-01 | 2.0.0 |
| `UHC_NY_NationalPPO` | 5,302,702 | `PP1 00` | 2026-08-01 | 2.0.0 |
| `UHC_NY_POSChoicePlus` | 5,044,110 | `PS1 50` | 2026-08-01 | 2.0.0 |
| `UHC_NY_SelectEPO` | 3,040,502 | `Select EPO` | 2026-08-01 | 2.0.0 |

Alongside the rate files, `payer_parquet/_tin_names/<payer>.parquet` holds a
small TIN → provider-name lookup (section 6). It lives in a subdirectory so the
non-recursive `*.parquet` glob never mixes it into the rate data.

---

## 2. Columns

| # | column | type | meaning |
|---|---|---|---|
| 1 | `billing_code` | VARCHAR | Procedure code as published, verbatim. |
| 2 | `code_type` | VARCHAR | Code system for `billing_code`. |
| 3 | `description` | VARCHAR | Payer's free-text label for the code. |
| 4 | `negotiated_rate` | DOUBLE | The rate. **Unit depends on `rate_type`.** |
| 5 | `rate_type` | VARCHAR | Rate methodology (source `negotiated_type`). |
| 6 | `billing_class` | VARCHAR | `professional` or `institutional`. |
| 7 | `service_codes` | VARCHAR | Place-of-service codes, pipe-joined. |
| 8 | `expiration_date` | VARCHAR | Date string, not a DATE. |
| 9 | `matched_npis` | VARCHAR | Comma-joined target NPIs this rate touches. |
| 10 | `matched_tins` | VARCHAR | Comma-joined target TINs this rate touches. |
| 11 | `group_tins` | BIGINT | Count of distinct TINs sharing the rate (fan-out). |
| 12 | `network_names` | VARCHAR | Pipe-joined networks of the matched groups. |
| 13 | `payer` | VARCHAR | Config label; equals the filename stem. |
| 14 | `reporting_entity_name` | VARCHAR | Source header: the filing legal entity. |
| 15 | `last_updated_on` | VARCHAR | Source header: the file's as-of date. |
| 16 | `schema_version` | VARCHAR | Source header `version` (CMS schema rev). |
| 17 | `systems` | VARCHAR | Comma-joined health-system names, sorted. |
| 18 | `system_count` | BIGINT | Number of names in `systems`. |

Columns 1–12 come from the MRF body. 14–16 come from the source file header.
13, 17 and 18 are added by `run_pipeline.py` from `target_providers.csv` /
`target_tins.csv`.

### Grain

One row per `(in_network item x negotiated_rates block x negotiated_prices
entry)` that touches at least one target provider. **The row is not the grain
of a contract**: the provider-group boundary is dissolved into `matched_npis` /
`matched_tins`, so several source rate blocks collapse onto the same apparent
key.

There is no unique key and no surrogate id. On the earlier 10-file corpus, 45%
of logical keys carried more than one distinct `negotiated_rate` — up to 1,056
rates on one key. Nothing about the UHC files changes that shape. **Any join
that assumes one rate per key will fan out.** Exact duplicate rows also exist
within files (all columns equal). `COUNT(*)` is not a count of distinct priced
facts; deduplicate explicitly.

---

## 3. Billing codes

`code_type` is the source's `billing_code_type`, uppercase, unnormalised. Nine
values appear, and **the vocabulary varies sharply by payer**:

| `code_type` | rows | distinct codes | payers (of 18) | format |
|---|---:|---:|---:|---|
| `CPT` | 43,403,225 | 11,479 | 18 | 5 chars, incl. Category II/III (`0001F`, `0806T`) |
| `HCPCS` | 10,435,856 | 6,677 | 18 | 5 chars (`A0021`–`V5364`) |
| `MS-DRG` | 472,726 | 1,567 | 16 | 3 chars, zero-padded |
| `LOCAL` | 143,614 | 4,311 | 5 (Aetna only) | 3–11 chars, payer-proprietary |
| `CDT` | 139,918 | 785 | 7 | 5 chars (`D0120`–`D9997`) |
| `RC` | 32,949 | 324 | 16 | 4 chars, zero-padded |
| `CSTM-ALL` | 6,937 | 120 | 11 | payer-proprietary custom codes |
| `HIPPS` | 858 | 101 | 4 | 5 chars |
| `ICD` | 256 | 18 | 9 | 6–7 chars, **contains `.`** |

Only `CPT` and `HCPCS` are present in all 18 files. The CMS schema permits
**seventeen** code types — the nine above plus `NDC`, `R-DRG`, `S-DRG`,
`APS-DRG`, `AP-DRG`, `APR-DRG`, `APC` and `EAPG`. Do not hardcode the observed
nine.

**Join hazards on billing code:**

- **Leading zeros are significant and present.** Casting `billing_code` to an
  integer anywhere destroys the join — `CPT`, `MS-DRG` and `RC` all contain
  zero-prefixed codes.
- **`billing_code` alone is not unique across code systems.** `A5102` and
  `A9999` each appear as both `HCPCS` and `HIPPS`. Always join on the pair
  `(code_type, billing_code)`.
- **`ICD` codes carry a decimal point** (`Z01.82`, `Z01.818`). Reference
  tables storing ICD-10 without the dot will not match.
- **`LOCAL` and `CSTM-ALL` are payer-proprietary and mutually
  untranslatable.** `CSTM-ALL` is no longer a single placeholder code — UHC
  publishes **120 distinct custom codes** under it. Exclude both from any
  cross-payer comparison.
- **`description` is not stable and must never be a join key.** The same
  `(code_type, billing_code)` carries up to 3 different descriptions.

---

## 4. Rates

`negotiated_rate` is DOUBLE, never null, never negative. **Its unit is not
constant**, and this is the easiest way to produce a wrong number:

| `rate_type` | rows | min | median | max | unit |
|---|---:|---:|---:|---:|---|
| `negotiated` | 37,550,771 | 0.01 | 398.37 | 2,735,700.00 | dollars |
| `fee schedule` | 13,250,351 | 0.00 | 189.86 | 1,530,702.75 | dollars |
| `derived` | 2,732,207 | 0.00 | 188.03 | 220,853.84 | dollars |
| `percentage` | 1,045,289 | 10.00 | 58.00 | 100.00 | **percent of billed charges** |
| `per diem` | 57,721 | 4.00 | 3,363.00 | 89,468.00 | dollars **per day** |

- **`percentage` rows are percentages, not dollars.** All 1,045,289 fall in
  10–100. Averaging them with dollar rows silently drags every median down.
- **`per diem` rows are per-day, not per-case**, and not comparable to a fee
  schedule line without a length-of-stay assumption.
- **Zero rates are placeholders.** 436,200 rows are exactly `0.00`, and
  **every one of them is Aetna** — 329,983 in `Aetna_NY` alone (25% of that
  file). Cigna and UHC have none.
- **The methodology mix shifted completely when UHC landed.** On the
  Aetna+Cigna corpus, `fee schedule` was the majority. With UHC included,
  `negotiated` is 69% of all rows. **Any conclusion drawn from the earlier mix
  does not carry forward.** Per payer the split still varies hugely: Cigna is
  ~98.7% `fee schedule`; `AetnaALIC_Hmo` has no `fee schedule` rows at all.

`billing_class` is always exactly `professional` (48,727,141) or
`institutional` (5,909,198), never empty. The CMS schema also permits `both`,
which no file uses — don't write a two-branch conditional that assumes it
cannot appear.

---

## 5. Provider identity

There is **no provider dimension and no provider-group id in the output.**
`provider_group_id` is consumed in pass 1 and discarded. Identity survives as
three derived columns plus a side lookup:

- **`matched_npis`** — comma-joined, sorted, 10-digit NPIs, and **only the
  target NPIs that matched**. Empty when the row matched by TIN alone.
- **`matched_tins`** — comma-joined, sorted, 9-digit TINs, **no dashes**, only
  the target TINs that matched. All observed tokens are 9-character EINs, but
  the CMS schema allows `tin.type` to be `npi`, in which case the value is a
  10-digit NPI. The parser does not record which type it was, so **if a payer
  ever uses NPI-type TINs this column silently mixes two namespaces.**
- **`group_tins`** — an *integer count*, not a list: how many distinct tax IDs
  the rate is shared with. Range **1 to 514,491; never 0** in any file.

**Join hazards on provider:**

- **Both identity columns are multi-valued strings, not keys.** Split and
  unnest before joining, and expect rows to duplicate.
- **79.4% of rows have an empty `matched_npis`.** Provenance: TIN-only
  43,403,187 (79.4%), both 10,569,044 (19.3%), NPI-only 664,108 (1.2%).
  Every row has at least one matched identifier, but **an NPI-keyed join drops
  four fifths of the data.** TIN is the reliable provider key.
- **The rate is often not attributable to one provider.** `group_tins` above
  1,000 covers 5,173,495 rows — the payer's network-wide fee schedule, shared
  with up to half a million tax IDs, not a contract with the matched system.
  Distribution: 15,108,079 rows at exactly 1; 18,852,460 at 2–10; 11,042,530 at
  11–100; 4,459,775 at 101–1000; 5,173,495 above 1,000. **Filter on
  `group_tins` before concluding anything about what a payer pays a system.**

### `_tin_names/` — the provider-name lookup

`payer_parquet/_tin_names/<payer>.parquet` carries `(tin, business_name,
payer)`: **1,646 rows across 120 distinct TINs and all 18 payers.**

It is a lookup, not a column, deliberately: **the payer-supplied name is not a
name.** Aetna packs every practice under a tax ID into one semicolon-joined
string — the longest is **3,292 characters** (~100 practice names in a single
field), and the average is 80. As a rate column that would repeat multi-KB
strings across tens of millions of rows to carry 120 distinct values.

Treat it as a hint for eyeballing a TIN, not as a provider dimension. Your own
reviewed `target_tins.csv` mapping is cleaner and is what `systems` is built
from.

### `systems` / `system_count`

Comma-joined, sorted health-system names resolved from the matched NPIs and
TINs. **Never empty**; `system_count` is always at least 1. Seven base systems:
`Montefiore`, `Mount Sinai`, `NYP`, `NYU Langone`, `Northwell`, `WMC`,
`White Plains`.

**6,734,380 rows (12.3%) span more than one system**, up to all 7 at once.
Grouping by `systems` as if it were a single system is wrong for those rows.
`analyze.py` exposes the choice as `--attr explode` (credit every system,
counts over-sum) vs `--attr exclusive` (`system_count = 1` only, drops shared
rows). Pick one deliberately.

---

## 6. Payer, plan and network

The output now carries entity, network and as-of date. **Plan identity is still
absent, and cannot be recovered from these files.**

### What is present

`reporting_entity_name`, `last_updated_on` and `schema_version` are read from
the source header and are exact. They make three things visible that were
previously invisible:

- **`Aetna_NY` is two months stale** — `2026-06-05` against `2026-08-01` /
  `2026-08-05` everywhere else. Comparing it to the others compares different
  reporting months.
- **Cigna files on schema `2.0.1`**, Aetna and UHC on `2.0.0`.
- **Three distinct legal entities**, not two: Aetna Life Insurance Company,
  Aetna Health Insurance Company of New York, Cigna Health Life Insurance
  Company, UnitedHealthcare Insurance Company of New York.

### What is absent

No `plan_name`, `plan_id`, `plan_id_type` (HIOS / EIN) or `plan_market_type`.
These are **not in the source in-network files at all** — every file inspected
opens with exactly four header keys and goes straight into
`provider_references`. Plan identity lives in the payer's **index**, so it
cannot be recovered from the cached sources; `find_files.py` and the index are
the only route.

**A `payer` value is a file, not a plan.** One Cigna file backs 20,856 plans;
another backs 190. Rates cannot be resolved to a plan, employer or market
segment from this data.

### `network_names` — and where it is empty

Pipe-joined (not comma-joined: network names are free text and may contain
commas, while NPIs and TINs never can). It carries the networks of the
**matched** provider groups only.

**23,560,292 rows (43%) have an empty `network_names`, and all of them are
Aetna.** Aetna's `network_name` is an array and its provider groups span
several networks at once, so there is no single correct per-row value to
backfill — `AetnaALIC_OpenAccessElectChoice` touches 11 distinct networks,
`Aetna_NY` 8, `AetnaALIC_Ppo` 6. Those rows were parsed before the column
existed and the row-to-provider-group link is not stored, so **populating them
requires a full re-parse of those six Aetna sources** (~8–12 hours on observed
throughput). Cigna and UHC are single-network per file and carry real values.

**The `payer` label is not a reliable network name.** `AetnaALIC_Hmo` is the
proof: it is unambiguous — every one of its 170,963 rows sits in
**`Aetna Choice POS II`**, not an HMO network. The config label came from the
index entry `Aetna HMO_60178`. Where `network_names` is populated, trust it
over the label; where it is empty, the label is an approximation of a file that
spans several networks.

UHC's names are inconsistent in a different way: three are readable
(`Choice-EPO`, `Choice-Plus`, `Select EPO`) and three are raw index codes
(`EP1 50`, `PP1 00`, `PS1 50`). That is what UHC publishes, not a parsing
artifact.

### Duplicate and near-duplicate files

**Two Cigna pairs are exact duplicates.** `Cigna_PathwellOAP` is row-for-row
identical to `Cigna_NationalOAP`, and `Cigna_PathwellPPO` to
`Cigna_NationalPPO`, on every non-`payer`, non-`network_names` column
(`EXCEPT ALL` both directions: 0 rows differ). **2,717,020 rows — 5.0% of the
corpus — counted twice** by any glob.

This is Cigna's, not a pipeline defect. The four sources are distinct files of
distinct size with different `provider_group_id`s and different networks, and
the logs confirm each label read its own source. Pathwell is a steerage program
layered over the same rate table for these hospitals. Now that `network_names`
is populated, the duplication is visible rather than mysterious — the rows are
identical but tagged `PATHWELL OAP` vs `NATIONAL OAP`.

**Two UHC pairs look identical but are not.** `UHC_NY_ChoiceEPO` and
`UHC_NY_ChoicePlus` both hold exactly 3,006,815 rows; `UHC_NY_ChoiceEPO50` and
`UHC_NY_POSChoicePlus` both hold exactly 5,044,110. **They are different
files** — 271,739 rows differ in each direction for the first pair (9%) and
305,712 for the second (6%). **Identical row counts are not evidence of
duplication**; check content before dropping either.

---

## 7. Nulls and sentinel values

**No column in any file contains a single SQL NULL** — all 18 are effectively
NOT NULL across all 54,636,339 rows. Missingness is encoded as sentinels, so
`IS NULL` will not find it:

| column | sentinel | rows | meaning |
|---|---|---:|---|
| `matched_npis` | `''` | 43,403,187 (79.4%) | matched by TIN only |
| `network_names` | `''` | 23,560,292 (43.1%) | Aetna multi-network, not backfillable |
| `service_codes` | `'CSTM-00'` | 23,845,664 (43.6%) | CMS "same rate at every POS" |
| `expiration_date` | `'9999-12-31'` | 51,900,777 (95.0%) | no expiry / open-ended |
| `service_codes` | `''` | 1,276,509 (2.3%) | institutional rows |
| `matched_tins` | `''` | 664,108 (1.2%) | matched by NPI only |
| `negotiated_rate` | `0.0` | 436,200 (0.8%) | placeholder, Aetna only |
| `systems` | `''` | 0 | never empty |
| `group_tins` | `0` | 0 | documented as "unavailable"; never occurs |

Notes:

- `expiration_date` is a **VARCHAR, not a DATE**, and `'9999-12-31'` overflows
  a 32-bit date. Only 7 distinct values exist corpus-wide: the sentinel;
  `2026-12-31`, which is **exactly co-extensive with `rate_type='derived'`**
  (2,732,207 rows, both counts identical, Aetna only); and five real dates in
  early August 2026 (Cigna only, 3,355 rows).
- `service_codes = 'CSTM-00'` is **CMS-defined**, not a payer quirk — it means
  *this rate applies at every place of service*, not *this field is missing*.
  It must not be parsed as a POS code. Real values are pipe-joined two-digit
  CMS POS codes, up to 33 of them.
- Empty `service_codes` tracks institutional rows. This is
  standard-conformant: CMS requires `service_code` **only when `billing_class`
  is `professional`**.

---

## 8. Completeness and provenance

All 18 configured payers are parsed. What is still missing from the output:

- **No source URL, file name, or download timestamp.** `last_updated_on` dates
  the payer's file, but nothing records which URL produced which parquet, or
  when it was fetched. That link exists only in `config.py` and the lane logs,
  which are gitignored.
- **No plan identity** (section 6).
- **`network_names` empty on all six multi-network Aetna files.**
- A payer whose parse finds no target rates produces **no file**, so an absent
  file is ambiguous between "not parsed" and "parsed, zero matches". Only the
  lane log distinguishes them.

`payer_parquet/old_npi_only/` and `payer_parquet/trial_60tins/` hold superseded
outputs from earlier NPI-only and 60-TIN runs. They are **not part of this
contract**; a non-recursive glob already excludes them, as it does
`_tin_names/`.

---

## 9. Reading it safely

```sql
-- Dollar-comparable rates, deduplicated, excluding the duplicate Cigna pair
-- and network-wide fee schedules.
SELECT DISTINCT *
FROM read_parquet('payer_parquet/*.parquet')
WHERE payer NOT IN ('Cigna_PathwellOAP', 'Cigna_PathwellPPO')
  AND rate_type <> 'percentage'      -- percent of charges, not dollars
  AND negotiated_rate > 1            -- 0.00 placeholders
  AND group_tins <= 10               -- system contracts, not fee schedules
  AND code_type IN ('CPT', 'HCPCS', 'MS-DRG')   -- cross-payer comparable only
```

**These filters are verified against the CMS schema, not just inherited from
`analyze.py`** (checked against the [CMS Price Transparency Guide
in-network-rates schema](https://github.com/CMSgov/price-transparency-guide/blob/master/schemas/in-network-rates/README.md)):

- Excluding `percentage` is correct: CMS defines it as a percentage of billed
  charges entered as a whole number (40.5% is `40.5`, not `0.405`).
- Treating `per diem` as non-comparable is correct: CMS defines it as a
  **daily** rate whose reimbursement often varies with number of days.
- Filtering `negotiated_rate > 1` is defensible: the schema **does not sanction
  zero**, so those rows are non-conformant payer output, not free care.
- `9999-12-31` is the CMS-prescribed "no expiration" value.

Checklist for anything joining to this data:

1. Join billing codes on `(code_type, billing_code)` as **strings** — never
   cast to a number, never join on `description`.
2. Unnest `matched_tins` / `matched_npis` before joining a provider dimension,
   and prefer TIN — NPI is empty on 79.4% of rows.
3. Never treat `group_tins` as an identifier; it is a count.
4. Check `rate_type` before any arithmetic on `negotiated_rate`.
5. Test for `''`, `'9999-12-31'`, `'CSTM-00'` and `0.0` — not for NULL.
6. Decide `explode` vs `exclusive` attribution for the 12.3% multi-system rows.
7. Deduplicate: exact duplicate rows exist within files, and two Cigna files
   duplicate two others wholesale. Do **not** assume the UHC pairs with matching
   row counts are duplicates — they are not.
8. Use `network_names` over `payer` where it is populated; treat the 43% of
   rows where it is empty as "network unknown", not "no network".
9. Check `last_updated_on` before comparing payers — `Aetna_NY` is two
   reporting months behind everything else.
