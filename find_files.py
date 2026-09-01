"""
Pick the right in-network rate files out of a payer's Table of Contents.

Payer TiC portals list hundreds of files behind plan names that do not say
which network they cover. The index (Table of Contents) file does say: every
plan carries a `plan_market_type` of "group" (employer business, broad PPO
networks) or "individual" (ACA exchange, deliberately narrow). Picking an
individual/exchange file is the usual reason a parse comes back with almost
none of your providers in it.

This streams an index and reports the in-network files inside it, ranked by
how many plans reference each one. A file referenced by many group plans is
the broad network you want; one referenced by a single exchange plan is not.

The index is streamed, never loaded whole, so a multi-GB Anthem index works
the same as a 66 MB Cigna one.

Usage:
    python find_files.py <index_url_or_path>
    python find_files.py anthem_index.json.gz --market group
    python find_files.py <url> --medical --market group
    python find_files.py <url> --market group --contains PPO "Open Access"
    python find_files.py <url> --state NY --top 40
    python find_files.py <url> --market group --export candidates.csv

Then paste the URLs you want into config.PAYER_FILES and run:
    python run_pipeline.py --limit 50000
"""

import sys
import csv
from collections import defaultdict

from mrf_parser import open_source

try:
    import ijson.backends.yajl2_c as ijson
except Exception:  # pragma: no cover
    import ijson


MARKETS = ("group", "individual", "any")
# plan_id_type is a better signal than plan_market_type. Aetna labels every
# plan "group" even when the name says Exchange and the id is a HIOS
# marketplace id; an EIN is an actual employer.
ID_TYPES = ("ein", "hios", "any")
SAMPLE_PLANS = 3  # plan names kept per file, for the report

# Payers publish one rate file per benefit category, and the ancillary ones
# (dental, vision, chiropractic, behavioral) are referenced by EVERY plan --
# so ranking by plan count floats them to the top even though they hold no
# hospital rates. --medical drops them.
ANCILLARY = (
    "dental", "vision", "chiro", "acupuncture", "massage", "naturopath",
    "behavioral", "behavior-health", "ohbs", "ohph", "transplant",
    "speech", "_st_", "_crs_", "hearing", "podiatry",
)


def parse_args(argv):
    source = None
    market = "any"
    state = None
    id_type = "any"
    top = 25
    export = None
    medical = False
    contains = []
    i = 0
    while i < len(argv):
        a = argv[i]
        if a == "--market":
            i += 1
            if i >= len(argv):
                raise SystemExit(f"--market needs one of {MARKETS}")
            market = argv[i].lower()
            if market not in MARKETS:
                raise SystemExit(f"--market must be one of {MARKETS}, "
                                 f"got {argv[i]!r}")
        elif a == "--state":
            i += 1
            if i >= len(argv):
                raise SystemExit("--state needs a value, e.g. --state NY")
            state = argv[i].upper()
        elif a == "--top":
            i += 1
            if i >= len(argv):
                raise SystemExit("--top needs a number")
            try:
                top = int(argv[i])
            except ValueError:
                raise SystemExit(f"--top needs a number, got {argv[i]!r}")
        elif a == "--id-type":
            i += 1
            if i >= len(argv):
                raise SystemExit(f"--id-type needs one of {ID_TYPES}")
            id_type = argv[i].lower()
            if id_type not in ID_TYPES:
                raise SystemExit(f"--id-type must be one of {ID_TYPES}, "
                                 f"got {argv[i]!r}")
        elif a == "--medical":
            medical = True
        elif a == "--export":
            i += 1
            if i >= len(argv):
                raise SystemExit("--export needs a filename")
            export = argv[i]
        elif a == "--contains":
            # everything after --contains until the next flag is a keyword
            i += 1
            while i < len(argv) and not argv[i].startswith("--"):
                contains.append(argv[i].lower())
                i += 1
            continue
        elif source is None:
            source = a
        else:
            raise SystemExit(f"unexpected argument {a!r}")
        i += 1
    if source is None:
        raise SystemExit(
            "usage: python find_files.py <index_url_or_path> "
            "[--market group] [--contains PPO] [--state NY] [--top N] "
            "[--export out.csv]")
    return source, market, state, id_type, top, export, medical, contains


def scan_index(source, progress_every=5000):
    """
    Stream an index, aggregating by in-network file URL.

    Returns {location: {plans, markets, names, description}} where `plans`
    counts how many reporting plans point at that file.
    """
    files = defaultdict(
        lambda: {"plans": 0, "markets": set(), "id_types": set(),
                 "names": [], "description": ""})
    seen = 0

    src, closer = open_source(source)
    try:
        for struct in ijson.items(src, "reporting_structure.item"):
            plans = struct.get("reporting_plans", []) or []
            in_net = struct.get("in_network_files", []) or []

            markets = {str(p.get("plan_market_type", "")).lower()
                       for p in plans}
            markets.discard("")
            id_types = {str(p.get("plan_id_type", "")).lower() for p in plans}
            id_types.discard("")
            names = [str(p.get("plan_name", "")) for p in plans]

            for f in in_net:
                loc = f.get("location")
                if not loc:
                    continue
                rec = files[loc]
                rec["plans"] += len(plans) or 1
                rec["markets"] |= markets
                rec["id_types"] |= id_types
                rec["description"] = rec["description"] or str(
                    f.get("description", ""))
                for n in names:
                    if len(rec["names"]) < SAMPLE_PLANS and n not in rec["names"]:
                        rec["names"].append(n)

            seen += 1
            if progress_every and seen % progress_every == 0:
                print(f"  ... {seen:,} reporting structures, "
                      f"{len(files):,} distinct files", flush=True)
    finally:
        if closer:
            closer()
    return files, seen


def is_ancillary(loc):
    """True for dental/vision/behavioral-style files with no hospital rates."""
    name = loc.split("?")[0].split("/")[-1].lower()
    return any(k in name for k in ANCILLARY)


def matches(loc, rec, market, state, contains, medical=False, id_type="any"):
    if medical and is_ancillary(loc):
        return False
    if market != "any":
        if market not in rec["markets"]:
            return False
    if id_type != "any" and id_type not in rec.get("id_types", set()):
        return False
    hay = (loc + " " + rec["description"] + " " +
           " ".join(rec["names"])).lower()
    if state and f"_{state.lower()}_" not in hay and \
            f"-{state.lower()}-" not in hay and \
            f" {state.lower()} " not in hay:
        return False
    if contains and not any(k in hay for k in contains):
        return False
    return True


def short(loc, width=88):
    """The filename, which is the identifying part of a signed URL."""
    name = loc.split("?")[0].split("/")[-1]
    return name if len(name) <= width else name[:width - 3] + "..."


def main(argv):
    (source, market, state, id_type, top, export, medical,
     contains) = parse_args(argv)

    print(f"Scanning index: {source}")
    if market != "any":
        print(f"  filter: plan_market_type = {market}")
    if id_type != "any":
        print(f"  filter: plan_id_type = {id_type}"
              f"{'  (real employer plans)' if id_type == 'ein' else ''}")
    if medical:
        print("  filter: medical networks only (dental/vision/behavioral dropped)")
    if contains:
        print(f"  filter: name contains any of {contains}")
    if state:
        print(f"  filter: looks state-specific to {state}")
    print()

    files, seen = scan_index(source)
    print(f"\nRead {seen:,} reporting structures, "
          f"{len(files):,} distinct in-network files.")

    keep = {loc: rec for loc, rec in files.items()
            if matches(loc, rec, market, state, contains, medical, id_type)}
    print(f"{len(keep):,} match the filters.\n")

    if not keep:
        print("Nothing matched. Try relaxing --market / --contains / --state,")
        print("or run with no filters to see what the index actually holds.")
        return

    ranked = sorted(keep.items(), key=lambda kv: -kv[1]["plans"])

    print(f"Top {min(top, len(ranked))} by number of plans referencing them")
    print("(more plans = broader network = usually what you want)\n")
    for loc, rec in ranked[:top]:
        mk = ",".join(sorted(rec["markets"])) or "?"
        idt = ",".join(sorted(rec.get("id_types", ()))) or "?"
        print(f"  {rec['plans']:>6,} plans  [{mk}/{idt}]  {short(loc)}")
        if rec["names"]:
            print(f"          e.g. {'; '.join(rec['names'][:SAMPLE_PLANS])}")

    print("\nFull URL of the top match (paste into config.PAYER_FILES):\n")
    print(f'    {{"payer": "CHANGEME", "url": r"{ranked[0][0]}"}},')

    if export:
        with open(export, "w", newline="", encoding="utf-8") as fh:
            w = csv.writer(fh)
            w.writerow(["plans", "markets", "id_types", "filename",
                        "sample_plan_names", "url"])
            for loc, rec in ranked:
                w.writerow([rec["plans"], ",".join(sorted(rec["markets"])),
                            ",".join(sorted(rec.get("id_types", ()))),
                            short(loc, 999), "; ".join(rec["names"]), loc])
        print(f"\nWrote {len(ranked):,} candidates to {export}")


if __name__ == "__main__":
    main(sys.argv[1:])
