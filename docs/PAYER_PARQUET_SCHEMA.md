# `payer_parquet/` output contract

What `run_pipeline.py` writes, and what a downstream consumer may rely on.

Profiled 2026-09-05 with DuckDB 1.5.4 against the 10 completed files in
`payer_parquet/`. In-flight `.part` files were excluded (see
[Completeness](#8-completeness)). Every count below is measured, not estimated.

---

## 1. Shape

One Parquet file per entry in `config.PAYER_FILES`, named `<payer>.parquet`,
written by pyarrow (SNAPPY, `pandas` + `ARROW:schema` key/value metadata,
200k-row row groups). A file appears only when its parse finished: the writer
builds `<payer>.parquet.part` and renames on success, so **the presence of a
`.parquet` file is the completion signal**.

All 10 completed files share one identical schema — 14 columns, same names,
same types, same order. There is no partitioning and no manifest; the glob
`payer_parquet/*.parquet` is the dataset.

| file | rows |
|---|---:|
| `AetnaALIC_Hmo.parquet` | 170,963 |
| `AetnaALIC_OpenAccessElectChoice.parquet` | 5,925,265 |
| `AetnaALIC_OpenAccessHealthNetworkOption.parquet` | 59,248 |
| `AetnaALIC_OpenAccessManagedChoice.parquet` | 6,591,660 |
| `Aetna_NY.parquet` | 1,324,087 |
| `Cigna_LocalPlus.parquet` | 1,025,990 |
| `Cigna_NationalOAP.parquet` | 1,323,489 |
| `Cigna_NationalPPO.parquet` | 1,393,531 |
| `Cigna_PathwellOAP.parquet` | 1,323,489 |
| `Cigna_PathwellPPO.parquet` | 1,393,531 |
| **total** | **20,531,253** |

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
| 12 | `payer` | VARCHAR | Config label; equals the filename stem. |
| 13 | `systems` | VARCHAR | Comma-joined health-system names, sorted. |
| 14 | `system_count` | BIGINT | Number of names in `systems`. |

Columns 1–11 come from the MRF. Columns 12–14 are added by
`run_pipeline.py` at write time from `target_providers.csv` /
`target_tins.csv`.

### Grain

One row per `(in_network item x negotiated_rates block x negotiated_prices
entry)` that touches at least one target provider. **The row is not the
grain of a contract**: the provider-group boundary is dissolved into
`matched_npis` / `matched_tins`, so several source rate blocks collapse onto
the same apparent key.

There is no unique key and no surrogate id. Grouping all 10 files by
`(payer, billing_code, code_type, billing_class, rate_type, matched_tins,
matched_npis)` yields 5,319,586 keys, of which **2,390,843 (45%) carry more
than one distinct `negotiated_rate`** — up to 1,056 distinct rates on a single
key (`AetnaALIC_OpenAccessElectChoice`, CPT `00840`, `derived`, $3.39–$3,200).
Any join that assumes one rate per key will fan out.

Exact duplicate rows also exist *within* files (all 14 columns equal):

| payer | duplicate rows | % |
|---|---:|---:|
| `AetnaALIC_OpenAccessManagedChoice` | 970,115 | 14.7% |
| `AetnaALIC_OpenAccessElectChoice` | 519,067 | 8.8% |
| `Aetna_NY` | 235,499 | 17.8% |
| `Cigna_NationalPPO` / `Cigna_PathwellPPO` | 38,519 each | 2.8% |
| `Cigna_NationalOAP` / `Cigna_PathwellOAP` | 16,730 each | 1.3% |
| `Cigna_LocalPlus` | 10,568 | 1.0% |
| `AetnaALIC_Hmo` | 1,137 | 0.7% |
| `AetnaALIC_OpenAccessHealthNetworkOption` | 0 | 0% |

`COUNT(*)` is therefore not a count of distinct priced facts.

---

## 3. Billing codes

`code_type` is the source's `billing_code_type`, uppercase, unnormalised.
Nine values appear, and **the vocabulary varies by payer** — no payer emits
all nine. The CMS schema permits **seventeen**: the nine below plus `NDC`,
`R-DRG`, `S-DRG`, `APS-DRG`, `AP-DRG`, `APR-DRG`, `APC` and `EAPG`. Do not
hardcode the observed nine — the eight unseen values are legal and may arrive
with UHC.

| `code_type` | rows | distinct codes | payers (of 10) | format |
|---|---:|---:|---:|---|
| `CPT` | 18,256,164 | 11,471 | 10 | 5 chars, incl. Category II/III (`0001F`, `0806T`) |
| `HCPCS` | 1,810,226 | 6,668 | 10 | 5 chars (`A0021`–`V5364`) |
| `MS-DRG` | 270,251 | 771 | 8 | 3 chars, zero-padded (`001`–`999`) |
| `LOCAL` | 93,708 | 4,311 | 3 (Aetna only) | 3–11 chars, payer-proprietary |
| `CDT` | 84,639 | 785 | 5 | 5 chars (`D0120`–`D9997`) |
| `RC` | 15,247 | 317 | 8 | 4 chars, zero-padded (`0001`–`1006`) |
| `HIPPS` | 532 | 101 | 2 | 5 chars |
| `CSTM-ALL` | 280 | 1 | 5 (Cigna only) | literal `CSTM-00` |
| `ICD` | 206 | 18 | 7 | 6–7 chars, **contains `.`** (`Z01.82`) |

**Join hazards on billing code:**

- **Leading zeros are significant and present.** 459,544 `CPT` rows, 29,850
  `MS-DRG` rows and 15,149 `RC` rows start with `0`. Casting `billing_code`
  to an integer anywhere in the pipeline destroys the join.
- **`billing_code` alone is not unique across code systems.** `A5102` and
  `A9999` each appear as both `HCPCS` and `HIPPS`. Always join on the pair
  `(code_type, billing_code)`.
- **`ICD` codes carry a decimal point** (`Z01.82`, and 7-char `Z01.818`);
  76 rows contain a non-alphanumeric character. Reference tables that store
  ICD-10 without the dot will not match.
- **`LOCAL` and `CSTM-ALL` are payer-proprietary and mutually untranslatable.**
  Aetna's `LOCAL` (`ASTINCIDPHY`, `MATVAG1702`) and Cigna's single `CSTM-ALL`
  code have no cross-payer meaning. Exclude them from any payer comparison.
- **`description` is not stable and must never be a join key.** The same
  `(code_type, billing_code)` carries up to 3 different descriptions
  (e.g. `J1569`, `A4459`, `0806T`).

Cross-payer overlap on `(code_type, billing_code)`: 8,931 codes appear in all
10 files; 7,443 appear in exactly 3; 436 in exactly 2; 4 in exactly one.
Per-payer distinct code counts run from 8,981
(`AetnaALIC_OpenAccessHealthNetworkOption`) to 24,345
(`AetnaALIC_OpenAccessElectChoice` / `AetnaALIC_OpenAccessManagedChoice`).
**Comparing payers on the full code set compares different code sets** —
intersect first.

---

## 4. Rates

`negotiated_rate` is DOUBLE, never null, never negative. **Its unit is not
constant**, and this is the single easiest way to produce a wrong number:

| `rate_type` | rows | min | median | max | unit |
|---|---:|---:|---:|---:|---|
| `fee schedule` | 10,424,965 | 0.00 | 256.68 | 1,530,702.75 | dollars |
| `negotiated` | 7,935,016 | 0.01 | 1,326.90 | 2,735,700.00 | dollars |
| `derived` | 1,497,589 | 0.00 | 191.72 | 220,853.84 | dollars |
| `percentage` | 636,876 | 10.00 | 58.00 | 100.00 | **percent of billed charges** |
| `per diem` | 36,807 | 26.00 | 3,363.00 | 89,468.00 | dollars **per day** |

- **`percentage` rows are percentages, not dollars.** All 636,876 fall in
  10–100. Averaging them with dollar rows silently drags every median down.
  `analyze.py` excludes them via `rate_type <> 'percentage'`; anything reading
  this Parquet directly must do the same.
- **`per diem` rows are per-day, not per-case.** They are not comparable to a
  `fee schedule` line for the same code without a length-of-stay assumption.
- **Zero and near-zero rates are placeholders.** 389,355 rows are exactly
  `0.00`, concentrated in `Aetna_NY` (329,983 rows — **25% of that file**, all
  `rate_type='fee schedule'`), `AetnaALIC_OpenAccessManagedChoice` (31,243) and
  `AetnaALIC_OpenAccessElectChoice` (28,123). Cigna has none. A further 304,612
  rows fall between 0 and 1. `analyze.py` filters `negotiated_rate > 1`.
- **Methodology mix differs sharply by payer and is not a like-for-like
  comparison.** Cigna labels 98.4–98.8% of rows `fee schedule`; `Aetna_NY` labels
  62% `negotiated` and 37% `fee schedule`;
  `AetnaALIC_OpenAccessHealthNetworkOption` is 100% `negotiated`;
  `AetnaALIC_Hmo` is the only file with no `fee schedule` rows at all.

`billing_class` is always exactly `professional` or `institutional`, never
empty. `AetnaALIC_Hmo` and `AetnaALIC_OpenAccessHealthNetworkOption` contain
**no institutional rows at all** — they are professional-only files. Note the
CMS schema also permits a third value, **`both`**, which no completed file
uses; do not write a two-branch conditional that assumes it cannot appear when
UHC lands.

---

## 5. Provider identity

There is **no provider dimension and no provider-group id in the output.**
`provider_group_id` is consumed in pass 1 and discarded, as is the
`business_name` the source carries on **every** TIN (`"NEW LEBANON SNF LLC"`,
`"VIRTUA MEDICAL GROUP PA"`). So the output has no provider names at all: a
TIN cannot be read without an external lookup, even though the payer supplied
a name for it. Identity survives only as three derived columns:

- **`matched_npis`** — comma-joined, sorted, 10-digit NPIs, and **only the
  target NPIs that matched**. The other NPIs in the same provider group are
  dropped. Empty string when the row matched by TIN alone. Cardinality 0–46
  per row.
- **`matched_tins`** — comma-joined, sorted, 9-digit TINs, **no dashes**, and
  only the target TINs that matched. Cardinality 0–99 per row. All 82,230,040
  TIN tokens across the corpus are exactly 9 characters, i.e. all EINs — but
  the CMS schema allows `tin.type` to be **`npi`** as well as `ein`, in which
  case the value is a 10-digit NPI. The parser does not record which type it
  was, so **if a payer ever uses NPI-type TINs, this column will silently mix
  two identifier namespaces** with no way to tell them apart except length.
- **`group_tins`** — an *integer count*, not a list: how many distinct tax IDs
  the rate is shared with, summed over the provider groups it references. It
  is a fan-out width, not an identifier. Range 1–514,491; **never 0** in any
  completed file (0 is the documented "unavailable" value, and it does not
  occur here).

**Join hazards on provider:**

- **Both identity columns are multi-valued strings, not keys.** 6,134,331 rows
  (29.9%) carry more than one TIN; 2,794,263 rows (13.6%) carry more than one
  NPI.
  Joining on the raw string matches only rows with the identical member set —
  `count(DISTINCT matched_tins)` is 2,330 across just 120 real TINs. Split and
  unnest before joining, and expect the row to duplicate.
- **77.5% of rows have an empty `matched_npis`.** Provenance across the corpus:
  TIN-only 15,914,347 (77.5%), both 4,401,197 (21.4%), NPI-only 215,709 (1.0%).
  Neither column is ever empty on both sides — every row has at least one
  matched identifier — but **an NPI-keyed join silently drops three quarters
  of the data.** TIN is the reliable provider key here.
- **Coverage of the target lists is partial.** Only 151 distinct NPIs appear
  across all 20.5M rows, out of 940 in `target_providers.csv`. All 120
  `include=Y` TINs from `target_tins.csv` do appear. This is the expected
  consequence of the TIN-matching design, not a defect — but it means the NPI
  column cannot be treated as a census of the target providers.
- **The rate is not attributable to a single provider.** `group_tins` up to
  514,491 means the row is the payer's network-wide fee schedule, shared with
  half a million tax IDs, not a contract with the matched system. Filter on
  `group_tins` before drawing any conclusion about what a payer pays a
  specific system. `analyze.py` buckets it as 1 / 2–10 / 11–100 / 101–1000 /
  >1000. Distribution: 5,293,044 rows at exactly 1; 7,369,204 at 2–10;
  3,630,776 at 11–100; 998,305 at 101–1000; 3,239,924 above 1,000.

### `systems` / `system_count`

`systems` is a comma-joined, sorted list of health-system names resolved from
the matched NPIs and TINs. It is **never empty** and `system_count` is always
at least 1. 105 distinct combination strings occur over 7 base systems:
`Montefiore`, `Mount Sinai`, `NYP`, `NYU Langone`, `Northwell`, `WMC`,
`White Plains`.

13.1% of rows (2,685,682) span more than one system, up to all 7 at once.
**Grouping by `systems` as if it were a single system is wrong for those
rows** — it was the pre-existing bug that misattributed ~44% of rows.
`analyze.py` exposes the choice as `--attr explode` (credit every system,
counts over-sum) vs `--attr exclusive` (`system_count = 1` only, drops shared
rows). A consumer must pick one deliberately.

Single-system coverage is uneven: `NYU Langone` has exclusive rows in all 10
files, `Northwell` and `WMC` in 9, and `Montefiore`, `Mount Sinai`, `NYP` and
`White Plains` in only 8. An unfiltered per-system comparison compares
different payer sets.

---

## 6. Payer, plan and network — the biggest gap

**There is no plan or network identity in this output at all.** None of the
following reach the Parquet:

- no `network_name`
- no `reporting_entity_name` / `reporting_entity_type`
- no `last_updated_on`, no `version`
- no `plan_name`, `plan_id`, `plan_id_type` (HIOS / EIN), `plan_market_type`
- no source URL, file name, or download timestamp — **no provenance whatsoever**

Two different causes, and they matter differently (verified by reading the
headers of 4 Cigna and 3 Aetna source files):

- **Plan fields are absent from the source in-network files themselves.** Every
  file inspected opens with exactly four keys — `reporting_entity_name`,
  `reporting_entity_type`, `last_updated_on`, `version` — then goes straight
  into `provider_references`. No `plan_name`, `plan_id`, `plan_id_type` or
  `plan_market_type` anywhere in the header. Plan identity lives in the payer's
  **index**, not the rate file, so it cannot be recovered from the cached
  sources; `find_files.py` and the index are the only route to it.
- **Network and entity fields *are* in the source and are discarded by the
  parser.** `build_relevant_groups()` reads each `provider_references` item and
  keeps only `provider_group_id`, `npi` and `tin`. Everything else is dropped
  on the floor, including `network_name`, which is present on **every provider
  group in every file profiled**. `reporting_entity_name`, `last_updated_on`
  and `version` sit in the header and are never opened. These are recoverable
  cheaply — no re-download, and for the header fields no re-parse either.

The only identifier is `payer`, a **hand-authored label from
`config.PAYER_FILES`** that equals the filename stem. It is not a payer name,
not a network name, and not stable in any external sense: it encodes carrier,
entity and product by convention only (`AetnaALIC_OpenAccessManagedChoice` =
Aetna Life Insurance Company, Open Access Managed Choice). Renaming a label in
config orphans the existing file and forces a multi-GB re-parse.

Consequences a downstream consumer must handle:

- **A `payer` value is a *file*, not a plan.** One Cigna file backs 20,856
  plans (`Cigna_PathwellOAP`); another backs 190 (`Cigna_NationalPPO`). Rates
  cannot be resolved to a plan, an employer, or a market segment from this
  data. Joining a plan dimension to `payer` is not possible without going back
  to the source index.
- **A `payer` value is not one network either, and for Aetna it is not even
  close.** Aetna's `network_name` is an *array*: a single provider group in
  `AetnaALIC_OpenAccessHealthNetworkOption` carries `["AHF HMO", "HMO", "Open
  Access Aetna Health Network Only", "Open Access Aetna Health Network Option",
  "QPOS"]`, and one in `Aetna_NY` carries eight networks. The config label
  picks one of those names and applies it to the whole file. Cigna's
  `network_name` is single-valued (`"NATIONAL OAP"`, `"PATHWELL OAP"`,
  `"NATIONAL PPO"`, `"PATHWELL PPO"`) and does correspond 1:1 to the file.
  **Reading `payer` as "this rate belongs to this network" is wrong for Aetna
  and right for Cigna**, and nothing in the output tells you which case you
  are in.
- **Corporate entity is only implicit.** `Aetna_NY` is Aetna Health Insurance
  Company of New York; `AetnaALIC_*` is Aetna Life Insurance Company. They are
  different legal entities collapsed under a shared `Aetna` prefix, and
  `config.py` notes Aetna's declared `plan_market_type` cannot be trusted
  anyway.
- **There is no as-of date, though the source has one.** Every source file
  carries `last_updated_on` in its header — `2026-08-01` for all four Cigna
  files, `2026-08-05` for `AetnaALIC_*`, `2026-06-05` for `Aetna_NY` — and a
  `version` (`2.0.1` Cigna, `2.0.0` Aetna). Neither is read. Nothing in the
  Parquet dates the rates, so a consumer cannot tell a stale file from a
  current one, or detect that **`Aetna_NY` is two months older than everything
  else** and is being compared against August data.

### Two Cigna files are exact duplicates

`Cigna_PathwellOAP` is **row-for-row identical** to `Cigna_NationalOAP`, and
`Cigna_PathwellPPO` to `Cigna_NationalPPO`, on all 13 non-`payer` columns
(verified with `EXCEPT ALL` in both directions: 0 rows differ either way).

That is **2,717,020 rows — 13.2% of the corpus — counted twice.** Any
aggregate over `payer_parquet/*.parquet` double-weights those Cigna rates.
A consumer should drop the `Cigna_Pathwell*` pair (or the `Cigna_National*`
pair) before aggregating. `Cigna_LocalPlus` is genuinely distinct.

**This is not a pipeline defect.** Diagnosed 2026-09-05:

- The four Cigna sources are distinct files of distinct size —
  `national-oap` 1,591,586,833 bytes vs `pathwell-oap` 1,587,124,862;
  `national-ppo` 1,580,003,927 vs `pathwell-ppo` 1,580,340,294.
- They carry different `provider_group_id` values and different
  `network_name` values (`"NATIONAL OAP"` vs `"PATHWELL OAP"`).
- `download_run.log` and `lane_8.log` confirm each payer label was parsed from
  its own cache file. No label read the wrong source.

So Cigna genuinely publishes four separate network files, and **the negotiated
rates they contain for these 120 target TINs are identical between Pathwell and
National** — same rates, same `group_tins` fan-out, same provider-group
composition. Pathwell is a steerage program layered over the same underlying
rate table for these hospitals. The duplication is real and is Cigna's, not
ours.

What the pipeline contributes is that it is **invisible**: because
`network_name` is discarded, the two files are indistinguishable in the output
except by the `payer` label, so the duplication looks like a bug and has to be
re-diagnosed by anyone who finds it. Carrying `network_name` through would make
it self-evident and let a consumer dedupe deliberately instead of by
convention.

---

## 7. Nulls and sentinel values

**No column in any completed file contains a single SQL NULL** — all 14 are
effectively NOT NULL across all 20,531,253 rows. Missingness is encoded as
sentinels instead, so `IS NULL` checks will not find it:

| column | sentinel | rows | meaning |
|---|---|---:|---|
| `matched_npis` | `''` | 15,914,347 (77.5%) | matched by TIN only |
| `matched_tins` | `''` | 215,709 (1.1%) | matched by NPI only |
| `expiration_date` | `'9999-12-31'` | 19,030,309 (92.7%) | no expiry / open-ended |
| `service_codes` | `'CSTM-00'` | 11,115,953 (54.1%) | payer "all places" placeholder |
| `service_codes` | `''` | 198,072 (1.0%) | absent in source |
| `negotiated_rate` | `0.0` | 389,255 (1.9%) | placeholder, not a free service |
| `group_tins` | `0` | 0 | documented as "unavailable"; does not occur |

Notes:

- `expiration_date` is a **VARCHAR, not a DATE**, and `'9999-12-31'` overflows
  a 32-bit date. Cast defensively. Only 7 distinct values exist corpus-wide:
  the sentinel; `2026-12-31`, which is **exactly co-extensive with
  `rate_type='derived'`** (1,497,589 rows, Aetna only, both counts identical);
  and five real dates in early August 2026 (Cigna only, 3,355 rows total).
- `service_codes = 'CSTM-00'` is **not a place-of-service code and not a payer
  invention** — it is a value defined by the CMS schema, used "when rates are
  identical across all service codes". So it is semantically meaningful: it
  means *this rate applies at every place of service*, not *this field is
  missing*. It must not be parsed as a POS code. Real values are pipe-joined
  two-digit CMS POS codes (`02|19|21|...`), up to 33 of them.
- Empty `service_codes` is **exactly co-extensive with Cigna institutional
  rows** — 31,386 / 39,542 / 43,801 empty in `LocalPlus` / `*OAP` / `*PPO`,
  matching those files' institutional row counts exactly. This is
  standard-conformant, not a Cigna defect: CMS requires `service_code` **only
  when `billing_class` is `professional`**. Aetna supplies it regardless.
  Filtering on POS therefore drops all Cigna institutional rates.

---

## 8. Completeness

The dataset is **partial**. `config.PAYER_FILES` lists 18 payer files; 10 are
done.

- **Complete (10):** the files in the table in section 1 — 6 Aetna, 5 Cigna
  (2 of which are duplicates, section 6).
- **In progress (7 `.part` files):** `AetnaALIC_Epo`, `UHC_NY_ChoiceEPO`,
  `UHC_NY_ChoiceEPO50`, `UHC_NY_ChoicePlus`, `UHC_NY_NationalPPO`,
  `UHC_NY_POSChoicePlus`, `UHC_NY_SelectEPO`. A `.part` file is an open
  pyarrow writer handle: it is **not a readable Parquet file** (no footer) and
  must be skipped by any glob. The `.part` files currently on disk carry
  2026-09-02 timestamps while the running lanes started 2026-09-05 13:44 —
  they are orphans from an earlier interrupted run and will be overwritten.
- **Not started (1):** `AetnaALIC_Ppo`.
- **No UnitedHealthcare data exists in the completed set at all.** Every
  conclusion currently derivable from `payer_parquet/*.parquet` covers Aetna
  and Cigna only.

A payer whose parse finds no target rates produces **no file** (the writer
deletes the `.part`), so an absent file is ambiguous between "not yet parsed"
and "parsed, zero matches" — the lane log is the only place that distinction
is recorded.

`payer_parquet/old_npi_only/` and `payer_parquet/trial_60tins/` hold
superseded outputs from earlier NPI-only and 60-TIN runs. They are **not part
of this contract** and must be excluded from any read; a non-recursive
`payer_parquet/*.parquet` glob already does so.

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
`analyze.py`** (checked 2026-09-05 against the [CMS Price Transparency Guide
in-network-rates schema](https://github.com/CMSgov/price-transparency-guide/blob/master/schemas/in-network-rates/README.md)):

- Excluding `percentage` is correct: CMS defines it as "the negotiated
  percentage value ... for a percentage of billed charges arrangement", entered
  as a whole number (40.5% is `40.5`, not `0.405`). It is categorically not a
  dollar amount.
- Treating `per diem` as non-comparable is correct: CMS defines it as a **daily**
  dollar rate, and notes such arrangements "often have different contractual
  reimbursement rates depending on the number of days".
- Filtering `negotiated_rate > 1` is defensible: the schema defines the field as
  "the dollar or percentage amount" and **does not sanction zero**, so the
  389,355 `0.00` rows are non-conformant payer output, not free care.
- `9999-12-31` is the CMS-prescribed value for "no expiration", not a payer
  quirk — the schema instructs filers to use it when no expiration exists.

Checklist for anything joining to this data:

1. Join billing codes on `(code_type, billing_code)` as **strings** — never
   cast to a number, never join on `description`.
2. Unnest `matched_tins` / `matched_npis` before joining a provider dimension,
   and prefer TIN — NPI is empty on 77.5% of rows.
3. Never treat `group_tins` as an identifier; it is a count.
4. Check `rate_type` before any arithmetic on `negotiated_rate`.
5. Test for `''`, `'9999-12-31'`, `'CSTM-00'` and `0.0` — not for NULL.
6. Decide `explode` vs `exclusive` attribution for the 13.1% multi-system rows.
7. Deduplicate: exact duplicate rows exist within files, and two Cigna files
   duplicate two others wholesale.
8. Do not expect plan, network, entity, or as-of date — they are not in the
   output.
