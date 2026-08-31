"""
MRF pipeline orchestrator.

Reads a list of payer files from config, and for each one:
  1. Downloads it once to a local cache (skips if already present).
  2. Runs the two-pass NPI-filtered parser.
  3. Writes a compact Parquet file (skips if already parsed).

Re-runnable: a crash or Ctrl-C loses at most the file in flight. Everything
already parsed is left alone on the next run.

Usage:
    python run_pipeline.py

Edit config in config.py: TARGET_CSV, PAYER_FILES, and the cache/output dirs.
"""

import os
import sys
import time
import urllib.request

import pandas as pd

import config
from mrf_parser import (
    build_relevant_groups,
    stream_filtered_rates,
    BACKEND,
)


def log(msg):
    print(time.strftime("[%H:%M:%S] ") + msg, flush=True)


def load_targets(csv_path):
    df = pd.read_csv(csv_path, dtype=str)
    npis = set(df["npi"].astype(str))
    npi_to_system = dict(zip(df["npi"].astype(str), df["system"]))
    return npis, npi_to_system


def download_once(url, cache_dir):
    """Download url into cache_dir if not already there. Return local path."""
    os.makedirs(cache_dir, exist_ok=True)
    fname = url.split("/")[-1]
    local = os.path.join(cache_dir, fname)
    if os.path.exists(local) and os.path.getsize(local) > 0:
        log(f"  cached: {fname} ({os.path.getsize(local)/1e9:.2f} GB)")
        return local
    log(f"  downloading: {fname}")
    tmp = local + ".part"
    with urllib.request.urlopen(url, timeout=180) as resp, open(tmp, "wb") as out:
        while True:
            chunk = resp.read(1 << 20)
            if not chunk:
                break
            out.write(chunk)
    os.replace(tmp, local)
    log(f"  downloaded: {fname} ({os.path.getsize(local)/1e9:.2f} GB)")
    return local


def parse_one(payer, source, target_npis, npi_to_system, out_path):
    log(f"  pass 1: provider references...")
    state = {"n": 0}

    def p1(n):
        state["n"] = n

    rel = build_relevant_groups(source, target_npis, progress=p1)
    log(f"    kept {len(rel)} relevant provider groups")

    log(f"  pass 2: in-network rates...")
    rows = []
    last = time.time()
    for row in stream_filtered_rates(source, rel, target_npis):
        rows.append(row)
        if time.time() - last > 10:
            log(f"    ... {len(rows):,} rows so far")
            last = time.time()

    if not rows:
        log(f"  NO target rates found in {payer} "
            f"(provider groups matched: {len(rel)})")
        return 0

    df = pd.DataFrame(rows)
    df["payer"] = payer
    df["system"] = df["matched_npis"].str.split(",").str[0].map(npi_to_system)
    df.to_parquet(out_path, index=False)
    log(f"  wrote {out_path} ({len(df):,} rows)")
    return len(df)


def main():
    log(f"ijson backend: {BACKEND}")
    if BACKEND != "yajl2_c":
        log("  WARNING: fast C backend not active. "
            "pip install ijson with yajl for a big speedup.")

    target_npis, npi_to_system = load_targets(config.TARGET_CSV)
    log(f"loaded {len(target_npis)} target NPIs")

    os.makedirs(config.OUTPUT_DIR, exist_ok=True)
    total_rows = 0

    for pf in config.PAYER_FILES:
        payer = pf["payer"]
        url = pf["url"]
        out_path = os.path.join(config.OUTPUT_DIR, f"{payer}.parquet")

        log(f"=== {payer} ===")
        if os.path.exists(out_path):
            log(f"  already parsed: {out_path} (skipping)")
            continue

        try:
            if config.DOWNLOAD_FIRST and url.startswith("http"):
                source = download_once(url, config.CACHE_DIR)
            else:
                source = url  # local path, or stream the URL directly
            n = parse_one(payer, source, target_npis, npi_to_system, out_path)
            total_rows += n
        except KeyboardInterrupt:
            log("interrupted by user. progress on parsed files is saved.")
            sys.exit(1)
        except Exception as e:  # one bad file shouldn't kill the run
            log(f"  ERROR on {payer}: {type(e).__name__}: {e}")
            continue

    log(f"done. total rows across all payers: {total_rows:,}")
    log(f"parquet files in: {config.OUTPUT_DIR}/")


if __name__ == "__main__":
    main()
