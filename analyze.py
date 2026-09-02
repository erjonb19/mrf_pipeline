"""
Rate comparison across payers and systems, via DuckDB over the Parquet.

Attribution
-----------
A single negotiated rate can apply to a provider group spanning several
systems, so each row carries a `systems` list rather than one system. How to
credit those shared rows is a methodology choice, exposed as --attr:

  explode    (default) count a shared rate once for EVERY system it touches.
             Faithful to "this is what the payer pays this system", but the
             row counts across systems sum to more than the row total.

  exclusive  use only rows that touch exactly one system. No double counting
             and no ambiguity, at the cost of dropping shared rows entirely.

Run both. If they disagree sharply for a code, the comparison for that code
rests on shared provider groups and should be read with care.

Usage:
    python analyze.py                        # every code, explode
    python analyze.py 27447 99213            # specific billing codes
    python analyze.py --attr exclusive 27447
    python analyze.py --export 27447         # also write clean_comparison.csv
    python analyze.py --max-tins 10 27447    # system contracts only, not
                                             # network-wide fee schedules
"""

import sys
import glob

import duckdb

import config

# Rows that are actually dollar amounts. `percentage` rows are a percent of
# billed charges (a "50" means 50%, not $50) and placeholder rows come
# through as <= 1; both would drag every median down. Everything else --
# negotiated, fee schedule, per diem, derived -- is a real price. Cigna
# labels 95% of its rows `fee schedule`, so filtering to `negotiated` only
# (the old rule) threw Cigna away almost entirely.
COMPARABLE = "rate_type <> 'percentage' AND negotiated_rate > 1"

ATTR_MODES = ("explode", "exclusive")

# How many tax IDs share a rate. A rate that applies to one or a few TINs is
# a contract with that system; one shared by thousands is the payer's
# standard fee schedule that everyone in the network gets. Both are "what
# the payer pays the system", but they answer different questions.
BREADTH = """CASE
        WHEN group_tins <= 1    THEN '1  single TIN'
        WHEN group_tins <= 10   THEN '2  2-10 TINs'
        WHEN group_tins <= 100  THEN '3  11-100'
        WHEN group_tins <= 1000 THEN '4  101-1000'
        ELSE                         '5  >1000 (network-wide)'
    END"""


def sql_str(value):
    """Quote a value as a SQL string literal, escaping embedded quotes."""
    return "'" + str(value).replace("'", "''") + "'"


def parse_args(argv):
    export = False
    attr = "explode"
    max_tins = None
    codes = []
    i = 0
    while i < len(argv):
        a = argv[i]
        if a == "--export":
            export = True
        elif a == "--max-tins":
            i += 1
            if i >= len(argv):
                raise SystemExit("--max-tins needs a number, e.g. --max-tins 10")
            try:
                max_tins = int(argv[i])
            except ValueError:
                raise SystemExit(f"--max-tins needs a number, got {argv[i]!r}")
        elif a == "--attr":
            i += 1
            if i >= len(argv):
                raise SystemExit(f"--attr needs one of {ATTR_MODES}")
            attr = argv[i]
            if attr not in ATTR_MODES:
                raise SystemExit(f"--attr must be one of {ATTR_MODES}, "
                                 f"got {attr!r}")
        else:
            codes.append(a)
        i += 1
    return export, attr, (codes or None), max_tins


def code_clause(codes, prefix="AND"):
    if not codes:
        return ""
    code_list = ",".join(sql_str(c) for c in codes)
    return f"{prefix} billing_code IN ({code_list})"


def base_cte(glob_pat, attr, codes, max_tins=None):
    """
    A CTE named `rows` exposing one `system` column per attribution mode.
    max_tins, if set, keeps only rates shared by at most that many tax IDs
    (system-specific contracts rather than network-wide fee schedules).
    """
    tins = f"AND group_tins <= {int(max_tins)}" if max_tins else ""
    if attr == "exclusive":
        return f"""
        WITH rows AS (
            SELECT *, systems AS system
            FROM '{glob_pat}'
            WHERE system_count = 1 {tins} {code_clause(codes)}
        )"""
    return f"""
    WITH rows AS (
        SELECT *, UNNEST(string_split(systems, ',')) AS system
        FROM '{glob_pat}'
        WHERE system_count > 0 {tins} {code_clause(codes)}
    )"""


def main(argv):
    export, attr, codes, max_tins = parse_args(argv)
    g = f"{config.OUTPUT_DIR}/*.parquet"
    if not glob.glob(g):
        print(f"No parquet in {config.OUTPUT_DIR}/. Run run_pipeline.py first.")
        return

    con = duckdb.connect()

    cols = con.execute(f"DESCRIBE SELECT * FROM '{g}'").df()["column_name"]
    if "systems" not in list(cols):
        print("These parquet files predate the system-attribution fix.")
        print("Run:  python backfill_systems.py")
        return
    if "group_tins" not in list(cols):
        print("These parquet files predate TIN matching (no group_tins "
              "column). Delete them and re-run run_pipeline.py.")
        return

    print(f"\n== Attribution mode: {attr} ==")
    if max_tins:
        print(f"== Only rates shared by <= {max_tins} tax IDs (--max-tins) ==")

    print("\n== Shared-rate exposure (how much rests on multi-system groups) ==")
    print(con.execute(f"""
        SELECT system_count,
               COUNT(*) AS rows,
               ROUND(100.0 * COUNT(*) / SUM(COUNT(*)) OVER (), 1) AS pct
        FROM '{g}'
        WHERE 1=1 {code_clause(codes)}
        GROUP BY system_count
        ORDER BY system_count
    """).df().to_string(index=False))

    print("\n== Rate-type mix (what gets filtered out) ==")
    print(con.execute(f"""
        SELECT rate_type,
               billing_class,
               COUNT(*) AS rows,
               SUM(CASE WHEN negotiated_rate <= 1 THEN 1 ELSE 0 END) AS junk_le_1
        FROM '{g}'
        WHERE 1=1 {code_clause(codes)}
        GROUP BY rate_type, billing_class
        ORDER BY rows DESC
    """).df().to_string(index=False))

    print("\n== Rate breadth: how many tax IDs share each rate ==")
    print(con.execute(f"""
        SELECT {BREADTH} AS shared_by,
               payer,
               COUNT(*) AS rows,
               ROUND(100.0 * COUNT(*) / SUM(COUNT(*)) OVER (PARTITION BY payer), 1) AS pct
        FROM '{g}'
        WHERE {COMPARABLE} {code_clause(codes)}
        GROUP BY 1, 2
        ORDER BY payer, 1
    """).df().to_string(index=False))
    print("(single-TIN / few-TIN rows are system-specific contracts; "
          ">1000 is the payer's standard fee schedule. "
          "Use --max-tins N to restrict the comparison below.)")

    print("\n== Clean comparison (dollar rates only, by billing class) ==")
    clean = con.execute(f"""
        {base_cte(g, attr, codes, max_tins)}
        SELECT billing_code,
               billing_class,
               system,
               payer,
               COUNT(*)                          AS n,
               ROUND(MIN(negotiated_rate), 2)    AS rate_min,
               ROUND(quantile_cont(negotiated_rate, 0.25), 2) AS p25,
               ROUND(MEDIAN(negotiated_rate), 2) AS rate_median,
               ROUND(quantile_cont(negotiated_rate, 0.75), 2) AS p75,
               ROUND(MAX(negotiated_rate), 2)    AS rate_max,
               ROUND(MEDIAN(group_tins))         AS tins_sharing
        FROM rows
        WHERE {COMPARABLE}
        GROUP BY billing_code, billing_class, system, payer
        ORDER BY billing_code, billing_class, rate_median DESC
    """).df()
    print(clean.to_string(index=False))
    if attr == "explode":
        print("\nNote: under --attr explode a shared rate is counted for each "
              "system it touches, so `n` sums to more than the row total.")

    if export:
        out = "clean_comparison.csv"
        clean.to_csv(out, index=False)
        print(f"\nExported {len(clean)} rows to {out}")


if __name__ == "__main__":
    main(sys.argv[1:])
