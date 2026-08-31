"""
Diagnostic for the 0-rows case.

When a file's pass 1 finds relevant provider groups but pass 2 returns no
rates, run this to find out why. It samples the in_network block and reports:
  - whether rates use provider_references or inline provider_groups
  - how many referenced group IDs overlap your matched groups
  - whether inline NPIs hit your targets
  - the actual ID formats on both sides (to catch type mismatches)

Usage:
    python diagnose.py <payer_label>
    python diagnose.py Aetna_NY
"""

import sys

import pandas as pd

import config
from mrf_parser import open_source, build_relevant_groups
try:
    import ijson.backends.yajl2_c as ijson
except Exception:
    import ijson


def resolve_source(payer):
    for pf in config.PAYER_FILES:
        if pf["payer"] == payer:
            return pf["url"]
    raise SystemExit(f"payer '{payer}' not found in config.PAYER_FILES")


def diagnose(source, relevant_groups, target_npis, sample=1000):
    has_refs = has_inline = has_neither = 0
    ref_ids_seen, inline_npis_seen = set(), set()
    overlap = 0
    n = 0

    src, closer = open_source(source)
    try:
        for item in ijson.items(src, "in_network.item"):
            for rg in item.get("negotiated_rates", []):
                refs = rg.get("provider_references", [])
                groups = rg.get("provider_groups", [])
                if refs:
                    has_refs += 1
                    for x in refs:
                        rs = str(x)
                        ref_ids_seen.add(rs)
                        if rs in relevant_groups:
                            overlap += 1
                if groups:
                    has_inline += 1
                    for g in groups:
                        for npi in g.get("npi", []):
                            inline_npis_seen.add(str(npi))
                if not refs and not groups:
                    has_neither += 1
            n += 1
            if n >= sample:
                break
    finally:
        if closer:
            closer()

    print(f"\nSampled {n} in_network items")
    print(f"  rate groups WITH provider_references : {has_refs}")
    print(f"  rate groups WITH inline provider_groups: {has_inline}")
    print(f"  rate groups with NEITHER             : {has_neither}")
    print(f"  distinct reference IDs in sample     : {len(ref_ids_seen)}")
    print(f"  of those, overlapping matched groups : {overlap}")
    print(f"  sample reference IDs    : {sorted(list(ref_ids_seen))[:10]}")
    print(f"  matched group IDs (kept): {sorted(list(relevant_groups.keys()))[:10]}")
    inline_hits = inline_npis_seen & target_npis
    print(f"  inline NPIs hitting targets: {len(inline_hits)} {sorted(list(inline_hits))[:10]}")

    print("\nInterpretation:")
    if has_refs and overlap == 0 and has_inline == 0:
        print("  Rates use references, but NONE point to your providers'")
        print("  groups. This plan does not price your targets -> use a")
        print("  broader-network file for this payer.")
    elif has_inline and not inline_hits and has_refs == 0:
        print("  Rates are inline and none contain your NPIs -> wrong file,")
        print("  or providers genuinely absent from this network.")
    elif overlap > 0 or inline_hits:
        print("  Overlap exists -> the parser SHOULD produce rows. If it")
        print("  didn't, increase sample or check negotiated_value nulls.")
    else:
        print("  Mixed/empty signal. Try a larger --sample or another file.")


def main():
    if len(sys.argv) < 2:
        raise SystemExit("usage: python diagnose.py <payer_label>")
    payer = sys.argv[1]
    source = resolve_source(payer)

    df = pd.read_csv(config.TARGET_CSV, dtype=str)
    target_npis = set(df["npi"].astype(str))

    print(f"Building relevant provider groups for {payer}...")
    rel = build_relevant_groups(source, target_npis)
    print(f"  matched {len(rel)} provider groups containing target NPIs")
    diagnose(source, rel, target_npis)


if __name__ == "__main__":
    main()
