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
    python run_pipeline.py --only Cigna_PathwellOAP   # one payer (trial)
    python run_pipeline.py --download-only            # fetch, parse nothing

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
import pyarrow as pa
import pyarrow.parquet as pq

import config
from mrf_parser import (
    build_relevant_groups,
    read_header,
    stream_filtered_rates,
    BACKEND,
    HTTP_HEADERS,
)


def log(msg):
    print(time.strftime("[%H:%M:%S] ") + msg, flush=True)


CHUNK_ROWS = 200_000  # rows buffered before each Parquet row group

# Where the per-payer TIN -> business_name lookups go, under OUTPUT_DIR.
TIN_NAMES_DIR = "_tin_names"


def load_targets(csv_path):
    df = pd.read_csv(csv_path, dtype=str)
    npis = set(df["npi"].astype(str))
    npi_to_system = dict(zip(df["npi"].astype(str), df["system"]))
    return npis, npi_to_system


def load_target_tins(csv_path):
    """
    {tin: system} for the rows of target_tins.csv marked include=Y.

    Payers list hospitals under NPIs an NPPES name search never returns, so
    matching on NPI alone found ~10% of the target hospitals. Every provider
    group carries the tax ID it bills under; a hospital has a handful. The
    CSV is reviewed by hand (see README) -- rows marked ? or N are ignored.
    Returns {} if the file is missing, which means NPI-only matching.
    """
    if not os.path.exists(csv_path):
        return {}
    df = pd.read_csv(csv_path, dtype=str).fillna("")
    keep = df[df["include"].str.strip().str.upper() == "Y"]
    return dict(zip(keep["tin"].str.strip(), keep["system"].str.strip()))


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


def systems_for(npi_csv, npi_to_system, tin_csv="", tin_to_system=None):
    """Every distinct system a row's matched NPIs and TINs belong to, sorted."""
    found = {npi_to_system.get(n) for n in npi_csv.split(",") if n}
    if tin_to_system:
        found |= {tin_to_system.get(t) for t in tin_csv.split(",") if t}
    found.discard(None)
    found.discard("")
    return ",".join(sorted(found))


def write_tin_names(payer, tin_names, out_dir):
    """
    Write one payer's {tin: business_name} as a small side lookup.

    A name is ~30 characters and there are ~120 of them; on a rate column it
    would repeat across tens of millions of rows to carry a handful of
    distinct values. As a lookup it is one tiny file you can join when you
    want a readable name and ignore when you don't.

    One file per payer, not one shared file: the lanes run in parallel and
    would clobber a single output. They live in a subdirectory so the
    `payer_parquet/*.parquet` glob -- the rate dataset -- never picks them up.
    """
    if not tin_names:
        return None
    d = os.path.join(out_dir, TIN_NAMES_DIR)
    os.makedirs(d, exist_ok=True)
    df = pd.DataFrame(sorted(tin_names.items()),
                      columns=["tin", "business_name"])
    df["payer"] = payer
    path = os.path.join(d, f"{payer}.parquet")
    pq.write_table(pa.Table.from_pandas(df, preserve_index=False), path)
    return path


def parse_one(payer, source, target_npis, npi_to_system, out_path,
              max_records=None, tin_to_system=None):
    tin_to_system = tin_to_system or {}
    target_tins = frozenset(tin_to_system)

    # Read the file-level metadata first: it is 8 KB off the front, and it is
    # the only thing that dates the rates. Without it a consumer cannot tell
    # a fresh file from one two reporting months stale.
    hdr = read_header(source)
    log(f"    {hdr['reporting_entity_name'] or '(no entity)'} | "
        f"updated {hdr['last_updated_on'] or '?'} | "
        f"schema {hdr['version'] or '?'}")

    log("  pass 1: provider references...")
    last = [time.time()]

    def p1(seen, kept):
        # pass 1 is silent for many minutes on a big file; prove it is alive
        if time.time() - last[0] > 15:
            log(f"    ... scanned {seen:,} provider groups, {kept:,} relevant")
            last[0] = time.time()

    sizes = {}
    tin_names = {}
    rel = build_relevant_groups(source, target_npis, target_tins,
                                progress=p1, group_sizes=sizes,
                                tin_names=tin_names)
    by_tin = sum(1 for g in rel.values() if g["matched_tins"])
    log(f"    kept {len(rel)} relevant provider groups "
        f"({by_tin} matched by TIN, {len(rel) - by_tin} by NPI only)")

    log("  pass 2: in-network rates...")
    # Rows are written in chunks: TIN matching yields millions of rows per
    # file and holding them all as dicts before the DataFrame ran into
    # memory. The .part file becomes the Parquet only once complete, so a
    # crash mid-file leaves nothing the resume logic would mistake for done.
    part = out_path + ".part"
    writer = None
    total = multi = 0
    chunk = []
    last = time.time()

    def flush():
        nonlocal writer, total, multi, chunk
        if not chunk:
            return
        df = pd.DataFrame(chunk)
        chunk = []
        df["payer"] = payer
        df["reporting_entity_name"] = hdr["reporting_entity_name"]
        df["last_updated_on"] = hdr["last_updated_on"]
        df["schema_version"] = hdr["version"]
        # One rate can apply to a provider group spanning several systems,
        # so store every system it touches. Collapsing to one here (the old
        # behaviour) silently misattributed ~44% of rows; analyze.py decides
        # how to attribute them at query time.
        df["systems"] = [
            systems_for(n, npi_to_system, t, tin_to_system)
            for n, t in zip(df["matched_npis"], df["matched_tins"])]
        df["system_count"] = df["systems"].map(
            lambda v: len(v.split(",")) if v else 0)
        table = pa.Table.from_pandas(df, preserve_index=False)
        if writer is None:
            writer = pq.ParquetWriter(part, table.schema)
        writer.write_table(table)
        total += len(df)
        multi += int((df["system_count"] > 1).sum())

    try:
        for row in stream_filtered_rates(source, rel, target_npis,
                                         target_tins, max_records=max_records,
                                         group_sizes=sizes):
            chunk.append(row)
            if len(chunk) >= CHUNK_ROWS:
                flush()
            if time.time() - last > 10:
                log(f"    ... {total + len(chunk):,} rows so far")
                last = time.time()
        flush()
    finally:
        if writer is not None:
            writer.close()

    if total == 0:
        if os.path.exists(part):
            os.remove(part)
        log(f"  NO target rates found in {payer} "
            f"(provider groups matched: {len(rel)})")
        return 0

    os.replace(part, out_path)
    log(f"  wrote {out_path} ({total:,} rows, {multi:,} span >1 system)")
    names_path = write_tin_names(payer, tin_names, os.path.dirname(out_path))
    if names_path:
        log(f"  wrote {names_path} ({len(tin_names)} TIN names)")
    return total


def parse_args(argv):
    """
    Return (max_records, download_only, only). max_records is None if
    unlimited; only is a set of payer labels to restrict the run to, or None.
    """
    download_only = "--download-only" in argv
    only = None
    if "--only" in argv:
        i = argv.index("--only")
        only = set()
        for a in argv[i + 1:]:
            if a.startswith("--"):
                break
            only.add(a)
        if not only:
            raise SystemExit("--only needs one or more payer labels from "
                             "config.PAYER_FILES")
        unknown = only - {pf["payer"] for pf in config.PAYER_FILES}
        if unknown:
            raise SystemExit(f"--only: not in config.PAYER_FILES: "
                             f"{sorted(unknown)}")
    if "--limit" in argv:
        i = argv.index("--limit")
        if i + 1 >= len(argv):
            raise SystemExit("--limit needs a number, e.g. --limit 50000")
        try:
            return int(argv[i + 1]), download_only, only
        except ValueError:
            raise SystemExit(f"--limit needs a number, got {argv[i + 1]!r}")
    return None, download_only, only


def main(argv=()):
    max_records, download_only, only = parse_args(list(argv))
    log(f"ijson backend: {BACKEND}")
    if download_only:
        log("  --download-only: fetching every remote file into the cache, "
            "parsing nothing")
    if max_records:
        log(f"  --limit active: stopping each file at {max_records:,} rows "
            f"(output will be PARTIAL)")
    if BACKEND != "yajl2_c":
        log("  WARNING: fast C backend not active. "
            "pip install ijson with yajl for a big speedup.")

    target_npis, npi_to_system = load_targets(config.TARGET_CSV)
    log(f"loaded {len(target_npis)} target NPIs")
    tin_to_system = load_target_tins(config.TARGET_TINS_CSV)
    if tin_to_system:
        log(f"loaded {len(tin_to_system)} target TINs "
            f"(include=Y rows of {config.TARGET_TINS_CSV})")
    else:
        log(f"  WARNING: {config.TARGET_TINS_CSV} missing or has no "
            f"include=Y rows -- matching on NPI only, expect ~10% coverage")

    os.makedirs(config.OUTPUT_DIR, exist_ok=True)
    total_rows = 0

    for pf in config.PAYER_FILES:
        payer = pf["payer"]
        url = pf["url"]
        out_path = os.path.join(config.OUTPUT_DIR, f"{payer}.parquet")
        if only and payer not in only:
            continue

        log(f"=== {payer} ===")
        if os.path.exists(out_path) and not download_only:
            log(f"  already parsed: {out_path} (skipping)")
            continue

        try:
            if config.DOWNLOAD_FIRST and url.startswith("http"):
                source = download_once(url, config.CACHE_DIR)
            else:
                source = url  # local path, or stream the URL directly
            if download_only:
                continue
            n = parse_one(payer, source, target_npis, npi_to_system,
                          out_path, max_records=max_records,
                          tin_to_system=tin_to_system)
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
