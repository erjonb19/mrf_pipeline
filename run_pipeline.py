"""
MRF pipeline orchestrator.

Reads a list of payer files from config, and for each one:
  1. Downloads it once to a local cache (skips if already present).
  2. Runs the two-pass NPI-filtered parser.
  3. Writes a compact Parquet file (skips if already parsed).

Re-runnable: a crash or Ctrl-C loses at most the file in flight. Everything
already parsed is left alone on the next run.

Usage:
    python run_pipeline.py                 # parse every unparsed payer
    python run_pipeline.py --limit 50000   # stop each file after N rate rows

--limit is for trialling a newly added payer: it caps pass 2 so you find out
whether a file yields usable rows in minutes instead of hours. Output written
under --limit is partial, so delete that parquet before a real run.

Edit config in config.py: TARGET_CSV, PAYER_FILES, and the cache/output dirs.
"""

import os
import sys
import time
import urllib.request
from urllib.parse import urlparse, unquote

import pandas as pd

import config
from mrf_parser import (
    build_relevant_groups,
    stream_filtered_rates,
    BACKEND,
    HTTP_HEADERS,
)


def log(msg):
    print(time.strftime("[%H:%M:%S] ") + msg, flush=True)


def load_targets(csv_path):
    df = pd.read_csv(csv_path, dtype=str)
    npis = set(df["npi"].astype(str))
    npi_to_system = dict(zip(df["npi"].astype(str), df["system"]))
    return npis, npi_to_system


def cache_name(url):
    """
    Local filename for a URL.

    Payer MRF links are usually signed (Azure/S3) and carry a query string
    full of characters Windows will not accept in a filename, so take the
    path component only and whitelist what survives.
    """
    raw = unquote(urlparse(url).path).split("/")[-1]
    safe = "".join(c if (c.isalnum() or c in "._-") else "_" for c in raw)
    return safe[:150] or "mrf_download"


def download_once(url, cache_dir):
    """Download url into cache_dir if not already there. Return local path."""
    os.makedirs(cache_dir, exist_ok=True)
    fname = cache_name(url)
    local = os.path.join(cache_dir, fname)
    if os.path.exists(local) and os.path.getsize(local) > 0:
        log(f"  cached: {fname} ({os.path.getsize(local)/1e9:.2f} GB)")
        return local
    log(f"  downloading: {fname}")
    tmp = local + ".part"
    req = urllib.request.Request(url, headers=HTTP_HEADERS)
    with urllib.request.urlopen(req, timeout=180) as resp, open(tmp, "wb") as out:
        while True:
            chunk = resp.read(1 << 20)
            if not chunk:
                break
            out.write(chunk)
    os.replace(tmp, local)
    log(f"  downloaded: {fname} ({os.path.getsize(local)/1e9:.2f} GB)")
    return local


def systems_for(npi_csv, npi_to_system):
    """Every distinct system a row's matched NPIs belong to, sorted."""
    found = {npi_to_system.get(n) for n in npi_csv.split(",") if n}
    found.discard(None)
    return ",".join(sorted(found))


def parse_one(payer, source, target_npis, npi_to_system, out_path,
              max_records=None):
    log("  pass 1: provider references...")
    rel = build_relevant_groups(source, target_npis)
    log(f"    kept {len(rel)} relevant provider groups")

    log("  pass 2: in-network rates...")
    rows = []
    last = time.time()
    for row in stream_filtered_rates(source, rel, target_npis,
                                     max_records=max_records):
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
    # One rate can apply to a provider group spanning several systems, so
    # store every system it touches. Collapsing to one here (the old
    # behaviour) silently misattributed ~44% of rows; analyze.py now decides
    # how to attribute them at query time.
    df["systems"] = df["matched_npis"].map(
        lambda v: systems_for(v, npi_to_system))
    df["system_count"] = df["systems"].map(
        lambda v: len(v.split(",")) if v else 0)
    df.to_parquet(out_path, index=False)
    multi = int((df["system_count"] > 1).sum())
    log(f"  wrote {out_path} ({len(df):,} rows, {multi:,} span >1 system)")
    return len(df)


def parse_args(argv):
    """Return max_records (None if unlimited)."""
    if "--limit" in argv:
        i = argv.index("--limit")
        if i + 1 >= len(argv):
            raise SystemExit("--limit needs a number, e.g. --limit 50000")
        try:
            return int(argv[i + 1])
        except ValueError:
            raise SystemExit(f"--limit needs a number, got {argv[i + 1]!r}")
    return None


def main(argv=()):
    max_records = parse_args(list(argv))
    log(f"ijson backend: {BACKEND}")
    if max_records:
        log(f"  --limit active: stopping each file at {max_records:,} rows "
            f"(output will be PARTIAL)")
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
            n = parse_one(payer, source, target_npis, npi_to_system,
                          out_path, max_records=max_records)
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
    main(sys.argv[1:])
