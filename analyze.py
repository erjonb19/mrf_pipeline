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
"""

import sys
import glob

import duckdb

import config

# Rows that are actually comparable dollar amounts. Percentage-of-charge and
# placeholder rows come through as <= 1 and would drag every median down.
COMPARABLE = "rate_type = 'negotiated' AND negotiated_rate > 1"

ATTR_MODES = ("explode", "exclusive")


def sql_str(value):
    """Quote a value as a SQL string literal, escaping embedded quotes."""
    return "'" + str(value).replace("'", "''") + "'"


def parse_args(argv):
    export = False
    attr = "explode"
    codes = []
    i = 0
    while i < len(argv):
        a = argv[i]
        if a == "--export":
            export = True
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
    return export, attr, (codes or None)


def code_clause(codes, prefix="AND"):
    if not codes:
        return ""
    code_list = ",".join(sql_str(c) for c in codes)
    return f"{prefix} billing_code IN ({code_list})"


def base_cte(glob_pat, attr, codes):
    """
    A CTE named `rows` exposing one `system` column per attribution mode.
    """
    if attr == "exclusive":
        return f"""
        WITH rows AS (
            SELECT *, systems AS system
            FROM '{glob_pat}'
            WHERE system_count = 1 {code_clause(codes)}
        )"""
    return f"""
    WITH rows AS (
        SELECT *, UNNEST(string_split(systems, ',')) AS system
        FROM '{glob_pat}'
        WHERE system_count > 0 {code_clause(codes)}
    )"""


def main(argv):
    export, attr, codes = parse_args(argv)
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

    print(f"\n== Attribution mode: {attr} ==")

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

    print("\n== Clean comparison (negotiated dollars only, by billing class) ==")
    clean = con.execute(f"""
        {base_cte(g, attr, codes)}
        SELECT billing_code,
               billing_class,
               system,
               payer,
               COUNT(*)                          AS n,
               ROUND(MIN(negotiated_rate), 2)    AS rate_min,
               ROUND(quantile_cont(negotiated_rate, 0.25), 2) AS p25,
               ROUND(MEDIAN(negotiated_rate), 2) AS rate_median,
               ROUND(quantile_cont(negotiated_rate, 0.75), 2) AS p75,
               ROUND(MAX(negotiated_rate), 2)    AS rate_max
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
