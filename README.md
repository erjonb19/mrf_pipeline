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

- `config.py`       — edit this. Target CSV path, payer file list, dirs.
- `mrf_parser.py`   — core two-pass parser (don't need to touch).
- `run_pipeline.py` — orchestrator. `python run_pipeline.py`
- `analyze.py`      — DuckDB analysis. `python analyze.py [codes...]`
- `diagnose.py`     — why-is-this-file-empty tool. `python diagnose.py <payer>`

## Run

1. Put your `target_providers.csv` (the 940 NPIs from step 1) in this folder,
   or point `config.TARGET_CSV` at it.

2. Edit `config.PAYER_FILES` with the payer in-network file URLs. Get these
   from each payer's table of contents. Pick BROAD-network files (full PPO
   networks), usually 1-2 per payer. Don't add narrow employer plans.

3. Parse:
   ```bash
   python run_pipeline.py
   ```

4. Analyze:
   ```bash
   python analyze.py                 # everything
   python analyze.py 27447 99213     # specific billing codes
   ```

## If a file returns 0 rows

That means pass 1 matched your providers in the directory, but pass 2 found
no rates referencing them. Run:

```bash
python diagnose.py Aetna_NY
```

It tells you whether the rates simply don't price your providers (use a
broader-network file) or whether there's a structural quirk to handle.

## Notes

- Output rows carry `payer`, `system`, `matched_npis`, and `tins` columns,
  so payer-side and hospital-side data join on NPI/TIN + billing_code later.
- `DOWNLOAD_FIRST = True` in config trades disk for speed and resumability.
  Set it False to stream URLs directly without caching.
- The pipeline is safe to re-run. It skips any payer whose Parquet already
  exists, so you can add payers incrementally.
