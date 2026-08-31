"""
Recompute system attribution on already-parsed Parquet files.

Older runs stored a single `system` per row, taken from the first entry of
`matched_npis`. That was wrong whenever a rate applied to a provider group
spanning more than one system: the row was credited to one arbitrary system
and vanished from the others.

This rewrites those files in place with the correct columns:

    systems       comma-joined sorted list of every system the row touches
    system_count  how many that is

No re-parse is needed. `matched_npis` already holds every target NPI on the
row, and TARGET_CSV maps NPI -> system, so the fix is fully derivable from
what is already on disk.

Safe to re-run: files that already carry `systems` are left alone.

Usage:
    python backfill_systems.py --dry-run   # report, change nothing
    python backfill_systems.py             # rewrite, keeping .bak copies
    python backfill_systems.py --no-backup
"""

import glob
import os
import shutil
import sys

import pandas as pd

import config


def load_map(csv_path):
    df = pd.read_csv(csv_path, dtype=str)
    return dict(zip(df["npi"].astype(str), df["system"]))


def systems_for(npi_csv, npi_to_system):
    """Every distinct system a row's matched NPIs belong to, sorted."""
    found = {npi_to_system.get(n) for n in str(npi_csv).split(",") if n}
    found.discard(None)
    return ",".join(sorted(found))


def backfill(path, npi_to_system, dry_run=False, backup=True):
    df = pd.read_parquet(path)

    if "systems" in df.columns:
        print(f"  {os.path.basename(path)}: already backfilled, skipping")
        return False

    if "matched_npis" not in df.columns:
        print(f"  {os.path.basename(path)}: no matched_npis column, SKIPPING")
        return False

    new_systems = df["matched_npis"].map(
        lambda v: systems_for(v, npi_to_system))
    new_count = new_systems.map(lambda v: len(v.split(",")) if v else 0)

    multi = int((new_count > 1).sum())
    unmapped = int((new_count == 0).sum())

    changed = 0
    if "system" in df.columns:
        # a row is misattributed if the old single system is not the whole story
        changed = int((new_systems != df["system"].fillna("")).sum())

    print(f"  {os.path.basename(path)}: {len(df):,} rows")
    print(f"    spanning >1 system : {multi:,} ({multi / max(len(df), 1):.1%})")
    print(f"    rows whose attribution changes: {changed:,}")
    if unmapped:
        print(f"    WARNING: {unmapped:,} rows map to no known system")

    if dry_run:
        print("    (dry run, not written)")
        return False

    df["systems"] = new_systems
    df["system_count"] = new_count
    df = df.drop(columns=["system"], errors="ignore")

    if backup:
        shutil.copy2(path, path + ".bak")
    tmp = path + ".tmp"
    df.to_parquet(tmp, index=False)
    os.replace(tmp, path)
    print(f"    rewritten{' (.bak kept)' if backup else ''}")
    return True


def main(argv):
    dry_run = "--dry-run" in argv
    backup = "--no-backup" not in argv

    npi_to_system = load_map(config.TARGET_CSV)
    print(f"loaded {len(npi_to_system)} NPI -> system mappings")

    paths = sorted(glob.glob(f"{config.OUTPUT_DIR}/*.parquet"))
    if not paths:
        print(f"No parquet in {config.OUTPUT_DIR}/. Nothing to do.")
        return

    done = 0
    for p in paths:
        if backfill(p, npi_to_system, dry_run=dry_run, backup=backup):
            done += 1
    print(f"\n{done} file(s) rewritten.")


if __name__ == "__main__":
    main(sys.argv[1:])
