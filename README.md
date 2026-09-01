# MRF Pipeline

A local Python pipeline for extracting and comparing negotiated rates from
CMS Transparency-in-Coverage machine-readable files (payer side), filtered to
a target set of provider NPIs across all billing codes.

This replaces the interactive Colab notebooks. It runs as a plain script, is
resumable after a crash, and caches downloads so you never re-stream a file.

## What it does

For each payer file in `config.py`, it:

1. Downloads the file once to `mrf_cache/` (skips if already present).
2. Runs a two-pass, NPI-filtered parser (schema 2.0).
   - Pass 1: keep only provider groups containing a target NPI.
   - Pass 2: emit only rate rows touching a target provider.
3. Writes a compact Parquet file to `payer_parquet/` (skips if already done).

Then `analyze.py` queries all the Parquet with DuckDB to produce rate
comparisons by code, system, and payer.

## Setup

```bash
pip install -r requirements.txt
```

The parser uses ijson's C backend (`yajl2_c`) for speed. `pip install ijson`
ships it on most platforms; the pipeline prints which backend is active on
startup and falls back to pure Python if needed.

## Files

- `config.py`            — edit this. Target CSV path, payer file list, dirs.
- `find_files.py`        — pick broad-network rate files out of a payer's
                           Table of Contents. `python find_files.py <index_url>`
- `mrf_parser.py`        — core two-pass parser (don't need to touch).
- `run_pipeline.py`      — orchestrator. `python run_pipeline.py`
- `analyze.py`           — DuckDB comparison. `python analyze.py [codes...]`
- `detail.py`            — row-level dump/export. `python detail.py [codes...]`
- `diagnose.py`          — why-is-this-file-empty tool. `python diagnose.py <payer>`
- `backfill_systems.py`  — one-off repair for Parquet written before the
                           system-attribution fix. `python backfill_systems.py`
- `test_pipeline.py`     — smoke tests. `python -m unittest test_pipeline -v`

## Run

1. Put your `target_providers.csv` (the 940 NPIs from step 1) in this folder,
   or point `config.TARGET_CSV` at it.

2. Edit `config.PAYER_FILES` with the payer in-network file URLs. Use
   `find_files.py` to pick them (see "Choosing the right file" below) —
   guessing from a payer's HTML file listing is how you end up with a
   narrow exchange network.

3. Parse:
   ```bash
   python run_pipeline.py

   # trialling a payer you just added? cap pass 2 so you find out in
   # minutes whether the file yields usable rows:
   python run_pipeline.py --limit 50000
   ```
   Output written under `--limit` is partial. Delete that Parquet before a
   real run.

4. Analyze:
   ```bash
   python analyze.py                 # everything
   python analyze.py 27447 99213     # specific billing codes
   python analyze.py --export 27447  # also write clean_comparison.csv
   ```

5. Inspect the rows behind a number:
   ```bash
   python detail.py 27447                          # print (capped at 200)
   python detail.py --system "NYU Langone" 27447
   python detail.py --export 27447 99213           # full dump to detail.csv
   ```

## Choosing the right file

Payers publish two kinds of file, and the portals do not make the difference
obvious:

| kind | filename | what it is |
| ---- | -------- | ---------- |
| Table of Contents | `*_index.json.gz` | a catalogue of plans, pointing at rate files |
| In-Network Rates  | `*_pl-xxx-hr23_*.json.gz` | the actual rates, what this pipeline parses |

Inside the index, every plan carries a `plan_market_type`:

- **`group`** — employer business. Broad PPO networks. What you almost
  always want.
- **`individual`** — ACA exchange. Deliberately narrow networks. Parsing one
  of these is the usual reason a run comes back with almost none of your
  target providers in it.

That field is not shown anywhere in the payers' HTML file listings, so start
from the index instead:

```bash
python find_files.py <index_url_or_path> --medical --market group
python find_files.py <index_url> --medical --contains PPO "Choice Plus"
python find_files.py <index_url> --medical --export candidates.csv
```

Always pass `--medical`. Payers publish a separate rate file per benefit
category, and the ancillary ones (dental, vision, chiropractic, behavioral)
are referenced by *every* plan — so ranking by plan count floats them above
the medical networks you actually want. `--medical` drops them.

It streams the index (multi-GB indexes are fine, nothing is loaded whole)
and ranks in-network files by how many plans reference them — more plans
generally means a broader network. It prints a paste-ready `config.py` line
for the top match.

Payer index locations:

- **Aetna** — `health1.aetna.com`, the TiC app. Note the entity: *Aetna Life
  Insurance Company* writes the broad employer business; *Aetna Health
  Insurance Company of New York* and anything under a `...FI`/exchange brand
  code is narrow.
- **UnitedHealthcare** — `transparency-in-coverage.uhc.com`. Not
  `providermrf.uhc.com`, which serves Medicaid provider directories and drug
  formularies under a different regulation and contains no rates.
- **Anthem/Empire** — the index is a direct S3 object:
  `https://antm-pt-prod-dataz-nogbd-nophi-us-east1.s3.amazonaws.com/anthem/<date>_anthem_index.json.gz`
  The date moves and only the current month is served. The bucket denies
  listing, so a stale date returns `403 AccessDenied` rather than 404 — that
  means "wrong filename", not "forbidden". Walk the date forward a month at a
  time until one returns 200. Verified 2026-08-31:
  `2026-09-01_anthem_index.json.gz`, 10.65 GB.
- **Cigna** — `cigna.com/legal/compliance/machine-readable-files`

## System attribution

One negotiated rate can apply to a provider group spanning several systems.
In this dataset that is **44% of rows** — some touch four systems at once.
There is no single correct way to credit those rows, so the Parquet stores
the truth and the decision is made at query time:

| column         | meaning                                          |
| -------------- | ------------------------------------------------ |
| `systems`      | every system the rate touches, comma-joined       |
| `system_count` | how many that is                                  |

`analyze.py --attr` picks how to use them:

- **`explode`** (default) — count a shared rate once for every system it
  touches. Faithful to "this is what the payer pays this system", but the
  per-system counts sum to more than the row total.
- **`exclusive`** — use only rows touching exactly one system. No double
  counting, but shared rows are dropped entirely.

Run both. Where they disagree sharply for a code, that comparison rests on
shared provider groups and should be read with care.

`detail.py --system X` returns rows *touching* X, including shared ones; add
`--exclusive` to drop the shared rows.

> Parquet written before this fix carries a single `system` column instead.
> `analyze.py` and `detail.py` will refuse to run and tell you to fix it:
> ```bash
> python backfill_systems.py --dry-run   # report, change nothing
> python backfill_systems.py             # rewrite, keeping .bak copies
> ```
> No re-parse is needed — the correction is derivable from `matched_npis`.

## If a file returns 0 rows

That means pass 1 matched your providers in the directory, but pass 2 found
no rates referencing them. Run:

```bash
python diagnose.py Aetna_NY
python diagnose.py Aetna_NY --sample 20000   # first items are often atypical
```

It tells you whether the rates simply don't price your providers (use a
broader-network file) or whether there's a structural quirk to handle.

## Tests

```bash
python -m unittest test_pipeline -v
```

28 tests against synthetic in-network and index fixtures: both
provider-group shapes, multi-system attribution, the `negotiated_value`
fallback, gzip detection, filename sanitising, and index filtering. They need
no network and no payer data — run them before and after touching
`mrf_parser.py`.

## Notes

- Output rows carry `payer`, `systems`, `matched_npis`, and `tins` columns,
  so payer-side and hospital-side data join on NPI/TIN + billing_code later.
- Compression is detected from the file's first two bytes, not its
  extension — payers serve plain JSON from `.gz` URLs and vice versa.
- Signed payer URLs (Azure/S3) carry query strings full of characters
  Windows rejects in filenames; the cache name is sanitised for this.
- `DOWNLOAD_FIRST = True` in config trades disk for speed and resumability.
  Set it False to stream URLs directly without caching.
- The pipeline is safe to re-run. It skips any payer whose Parquet already
  exists, so you can add payers incrementally.
- `analyze.py` filters to `rate_type = 'negotiated' AND negotiated_rate > 1`.
  That currently drops ~30% of rows, mostly `fee schedule` entries priced at
  or below $1 (percentage-of-charge placeholders) — but also some real
  dollar amounts. Revisit that filter if fee-schedule rates matter to you.
