"""
Row-level detail export.

Unlike analyze.py (which aggregates and filters to clean comparable rates),
this dumps EVERY individual negotiated rate row, with rate_type and
billing_class labeled so nothing is hidden. Use it to inspect the actual
values behind a summary cell, or to export raw rows for Power BI / Excel.

Exports stream straight to disk via DuckDB COPY, so they do not load the whole
result set into memory (safe for millions of rows on a normal laptop).

Usage:
    python detail.py 27447 99213              # print these codes (capped on screen)
    python detail.py --system "NYU Langone" 27447   # one system + code
    python detail.py --export 27447 99213     # write detail.csv (full, uncapped)
    python detail.py --export                 # export EVERY row to detail.csv

Flags combine, e.g.:
    python detail.py --export --system "NYU Langone" 27447
"""

import sys
import glob

import duckdb

import config

CONSOLE_CAP = 200  # max rows printed to screen; --export writes everything


def parse_args(argv):
    export = False
    system = None
    codes = []
    i = 0
    while i < len(argv):
        a = argv[i]
        if a == "--export":
            export = True
        elif a == "--system":
            i += 1
            system = argv[i]
        else:
            codes.append(a)
        i += 1
    return export, system, (codes or None)


def main(argv):
    export, system, codes = parse_args(argv)
    g = f"{config.OUTPUT_DIR}/*.parquet"
    if not glob.glob(g):
        print(f"No parquet in {config.OUTPUT_DIR}/. Run run_pipeline.py first.")
        return

    where = ["1=1"]
    if codes:
        code_list = ",".join(f"'{c}'" for c in codes)
        where.append(f"billing_code IN ({code_list})")
    if system:
        safe = system.replace("'", "''")
        where.append(f"system = '{safe}'")
    where_sql = " AND ".join(where)

    cols = """payer, system, billing_code, code_type, description,
              billing_class, rate_type, negotiated_rate,
              service_codes, expiration_date, matched_npis, tins"""
    order = "billing_code, system, billing_class, rate_type, negotiated_rate"

    con = duckdb.connect()
    # keep memory low: stream, don't buffer for insertion order
    con.execute("SET preserve_insertion_order=false")

    # cheap count first (no materialization)
    n = con.execute(f"SELECT COUNT(*) FROM '{g}' WHERE {where_sql}").fetchone()[0]
    print(f"\nMatched {n:,} detail rows.")

    if export:
        out = "detail.csv"
        # COPY streams to disk; never builds a full in-memory DataFrame
        con.execute(f"""
            COPY (
                SELECT {cols} FROM '{g}'
                WHERE {where_sql}
                ORDER BY {order}
            ) TO '{out}' (HEADER, DELIMITER ',')
        """)
        print(f"Exported all {n:,} rows to {out}")
    else:
        # only ever pull CONSOLE_CAP rows into pandas
        df = con.execute(f"""
            SELECT {cols} FROM '{g}'
            WHERE {where_sql}
            ORDER BY {order}
            LIMIT {CONSOLE_CAP}
        """).df()
        print(df.to_string(index=False))
        if n > CONSOLE_CAP:
            print(f"\n... {n - CONSOLE_CAP:,} more rows not shown. "
                  f"Add --export to write the full set to detail.csv")


if __name__ == "__main__":
    main(sys.argv[1:])
