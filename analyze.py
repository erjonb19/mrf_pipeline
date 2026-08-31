import sys
import glob
import duckdb
import config

COMPARABLE = "rate_type = 'negotiated' AND negotiated_rate > 1"


def parse_args(argv):
    export = False
    codes = []
    for a in argv:
        if a == "--export":
            export = True
        else:
            codes.append(a)
    return export, (codes or None)


def code_clause(codes, prefix="AND"):
    if not codes:
        return ""
    code_list = ",".join(f"'{c}'" for c in codes)
    return f"{prefix} billing_code IN ({code_list})"


def main(argv):
    export, codes = parse_args(argv)
    g = f"{config.OUTPUT_DIR}/*.parquet"
    if not glob.glob(g):
        print(f"No parquet in {config.OUTPUT_DIR}/. Run run_pipeline.py first.")
        return

    con = duckdb.connect()

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
        FROM '{g}'
        WHERE {COMPARABLE} {code_clause(codes)}
        GROUP BY billing_code, billing_class, system, payer
        ORDER BY billing_code, billing_class, rate_median DESC
    """).df()
    print(clean.to_string(index=False))

    if export:
        out = "clean_comparison.csv"
        clean.to_csv(out, index=False)
        print(f"\nExported {len(clean)} rows to {out}")


if __name__ == "__main__":
    main(sys.argv[1:])