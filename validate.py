"""
Post-parse validation gate for the payer_parquet/ dataset.

Why this exists
---------------
The unit tests cover the parser's logic on a fixture. Nothing covered its
*output*. That gap is not theoretical: 23.5 million Aetna rows shipped with a
completely empty `network_names` column and nobody noticed for weeks, because
every test passed and the files looked fine from the outside. The re-parse
that fixed it cost six hours.

This checks the finished Parquet instead of the code that wrote it, and it is
built around the assumption that the next bug will be a different one -- so
the broadest check here is "is any column entirely empty", not "is
network_names entirely empty".

What it will not do
-------------------
It cannot tell you a rate is wrong. It tells you a file is structurally sound,
internally consistent, and comparable to its siblings. Ground truth for the
rates themselves is the payer's own file.

Exit status is 0 when every check passes and 1 when any FAIL fires, so this
can gate a run:

    python run_pipeline.py && python validate.py

Usage:
    python validate.py                        # every parquet in OUTPUT_DIR
    python validate.py Cigna_NationalOAP      # named payers only
    python validate.py --strict               # treat warnings as failures
    python validate.py --expect-month 2026-08 # require this reporting month
    python validate.py --quiet                # findings only, no pass lines
"""

import glob
import os
import sys
from collections import Counter

import duckdb

import config

FAIL, WARN, OK = "FAIL", "WARN", "ok"

# The consumer contract, as documented in docs/PAYER_PARQUET_SCHEMA.md. An
# extra column is as much a break as a missing one: downstream code does
# SELECT * into a DataFrame and positional assumptions are cheap to make.
EXPECTED_COLUMNS = {
    "billing_code": "VARCHAR",
    "code_type": "VARCHAR",
    "description": "VARCHAR",
    "negotiated_rate": "DOUBLE",
    "rate_type": "VARCHAR",
    "billing_class": "VARCHAR",
    "service_codes": "VARCHAR",
    "expiration_date": "VARCHAR",
    "matched_npis": "VARCHAR",
    "matched_tins": "VARCHAR",
    "group_tins": "BIGINT",
    "network_names": "VARCHAR",
    "payer": "VARCHAR",
    "reporting_entity_name": "VARCHAR",
    "last_updated_on": "VARCHAR",
    "schema_version": "VARCHAR",
    "systems": "VARCHAR",
    "system_count": "BIGINT",
}

# Blank on a single row is a broken join. These are the columns a consumer
# joins or partitions on, and none of them has a legitimate empty value.
JOIN_KEYS = ("billing_code", "code_type", "payer")

# Blank on EVERY row means the column never got populated -- the
# network_names failure. Split by whether an all-blank column is a defect or
# just a quiet payer: `matched_npis` is 100% blank on a file matched purely
# by tax ID, which is correct behaviour, not a bug.
MUST_HAVE_VALUES = ("billing_code", "code_type", "payer", "systems",
                    "reporting_entity_name", "last_updated_on",
                    "schema_version")
MAY_BE_ALL_BLANK = ("description", "rate_type", "billing_class",
                    "service_codes", "expiration_date", "matched_npis",
                    "matched_tins", "network_names")

# One value per file, or the constant-column write in run_pipeline.flush()
# went wrong and rows from two sources are mixed together.
CONSTANT_COLUMNS = ("payer", "reporting_entity_name", "last_updated_on",
                    "schema_version")

# A zero-dollar rate is legal in an MRF -- it usually means "bundled, not
# separately payable" -- so a handful is normal. A quarter of the file is
# not, and Aetna_NY sits at 24.9%, which is worth knowing before anyone
# takes a median over it.
ZERO_RATE_WARN_PCT = 5.0


class Findings:
    """Collects (level, check, message) and knows whether the run failed."""

    def __init__(self, strict=False):
        self.items = []
        self.strict = strict

    def add(self, level, check, message):
        self.items.append((level, check, message))

    def count(self, level):
        return sum(1 for lvl, _, _ in self.items if lvl == level)


def scalar(con, sql):
    return con.sql(sql).fetchone()


def check_schema(con, src, f):
    """Column names and types match the contract exactly."""
    actual = {r[0]: r[1] for r in
              con.sql("DESCRIBE SELECT * FROM " + src).fetchall()}
    missing = [c for c in EXPECTED_COLUMNS if c not in actual]
    extra = [c for c in actual if c not in EXPECTED_COLUMNS]
    wrong = ["%s is %s, expected %s" % (c, actual[c], t)
             for c, t in EXPECTED_COLUMNS.items()
             if c in actual and actual[c] != t]
    if missing:
        f.add(FAIL, "schema", "missing columns: " + ", ".join(missing))
    if extra:
        f.add(FAIL, "schema", "unexpected columns: " + ", ".join(extra))
    for w in wrong:
        f.add(FAIL, "schema", w)
    if not (missing or extra or wrong):
        f.add(OK, "schema", "%d columns as contracted" % len(actual))
    return not missing


def check_join_keys(con, src, f):
    """No row may be blank in a column a consumer joins on."""
    bad = False
    for col in JOIN_KEYS:
        n, = scalar(con, "SELECT COUNT(*) FROM %s WHERE %s IS NULL OR %s = ''"
                    % (src, col, col))
        if n:
            bad = True
            f.add(FAIL, "join_keys", "%s blank on %s rows" % (col, f"{n:,}"))
    if not bad:
        f.add(OK, "join_keys",
              "%s populated on every row" % ", ".join(JOIN_KEYS))


def check_provider_identity(con, src, f):
    """
    Every row must name a provider by NPI or by TIN.

    stream_filtered_rates only yields a row when one of the two matched, so a
    row with neither means the filter regressed and the file now contains
    rates for providers nobody asked for.
    """
    n, = scalar(con, """SELECT COUNT(*) FROM %s
        WHERE (matched_npis IS NULL OR matched_npis = '')
          AND (matched_tins IS NULL OR matched_tins = '')""" % src)
    if n:
        f.add(FAIL, "provider_identity",
              "%s rows carry neither an NPI nor a TIN" % f"{n:,}")
    else:
        f.add(OK, "provider_identity", "every row names a provider")


def check_rates(con, src, f):
    """Rates are present, non-negative, and not overwhelmingly zero."""
    nulls, neg, zero, total = scalar(con, """SELECT
        SUM(CASE WHEN negotiated_rate IS NULL THEN 1 ELSE 0 END),
        SUM(CASE WHEN negotiated_rate < 0 THEN 1 ELSE 0 END),
        SUM(CASE WHEN negotiated_rate = 0 THEN 1 ELSE 0 END),
        COUNT(*) FROM %s""" % src)
    if nulls:
        f.add(FAIL, "rates", "negotiated_rate is NULL on %s rows" % f"{nulls:,}")
    if neg:
        f.add(FAIL, "rates",
              "negotiated_rate is negative on %s rows" % f"{neg:,}")
    pct = 100.0 * zero / total if total else 0.0
    if pct > ZERO_RATE_WARN_PCT:
        f.add(WARN, "rates", "%s rows (%.1f%%) have a zero rate; exclude "
              "them before taking a median" % (f"{zero:,}", pct))
    if not (nulls or neg):
        f.add(OK, "rates", "all rates present and non-negative "
              "(%s zero, %.1f%%)" % (f"{zero:,}", pct))


def check_constants(con, src, f, payer):
    """The file-level columns hold exactly one value, and it is the right one."""
    bad = False
    for col in CONSTANT_COLUMNS:
        n, = scalar(con, "SELECT COUNT(DISTINCT %s) FROM %s" % (col, src))
        if n != 1:
            bad = True
            f.add(FAIL, "constants",
                  "%s holds %d distinct values, expected 1" % (col, n))
    actual, = scalar(con, "SELECT ANY_VALUE(payer) FROM " + src)
    if actual != payer:
        bad = True
        f.add(FAIL, "constants", "payer column says %r but the file is "
              "named %r" % (actual, payer))
    if not bad:
        f.add(OK, "constants", "payer, entity, as-of date and schema "
              "version constant across the file")


def check_blank_columns(con, src, f):
    """
    Any column that is blank on every single row.

    This is the check that would have caught network_names. It is written
    against the whole column list on purpose -- naming the column that failed
    last time would only ever catch the bug we already fixed.
    """
    for col in MUST_HAVE_VALUES + MAY_BE_ALL_BLANK:
        n, total = scalar(con, "SELECT SUM(CASE WHEN %s IS NULL OR %s = '' "
                          "THEN 1 ELSE 0 END), COUNT(*) FROM %s"
                          % (col, col, src))
        if total and n == total:
            level = FAIL if col in MUST_HAVE_VALUES else WARN
            f.add(level, "blank_column",
                  "%s is empty on all %s rows" % (col, f"{total:,}"))


def check_month(con, src, f, expect_month, majority_month):
    """
    The reporting month, against an expected value or against its siblings.

    Payers republish monthly and a stale file is invisible in the data: the
    rates look perfectly valid, they are just two months old. Aetna_NY sat at
    2026-06-05 while every other file was 2026-08, which no other check here
    would have surfaced.
    """
    month, = scalar(con, "SELECT ANY_VALUE(last_updated_on) FROM " + src)
    if expect_month:
        if not (month or "").startswith(expect_month):
            f.add(FAIL, "as_of", "last_updated_on is %r, expected %s"
                  % (month, expect_month))
        else:
            f.add(OK, "as_of", "reporting date " + month)
    elif majority_month and not (month or "").startswith(majority_month):
        f.add(WARN, "as_of", "reporting date %s is behind the rest of the "
              "dataset (%s)" % (month, majority_month))
    else:
        f.add(OK, "as_of", "reporting date " + str(month))


# Everything that describes what a rate is and who it is for. Two files
# agreeing on all of it are the same rate table, whatever they are called.
# `network_names` is deliberately NOT in here -- it is the one column that is
# *supposed* to differ between two labels of one table, and keying on it
# would hide exactly the duplicates worth finding.
IDENTITY_COLUMNS = ("billing_code", "code_type", "description",
                    "negotiated_rate", "rate_type", "billing_class",
                    "service_codes", "expiration_date", "matched_npis",
                    "matched_tins", "group_tins", "systems")

COMPARED_COLUMNS = IDENTITY_COLUMNS + ("network_names",)


def content_signature(con, src):
    """
    Per-column fingerprints for spotting a file published twice under two names.

    Row count alone is not enough, and neither is folding in the rates.
    Measured across the four same-row-count pairs in this dataset:

      Cigna National/Pathwell  differ in network_names and NOTHING else --
                               one rate table, two steerage labels.
      UHC ChoiceEPO/ChoicePlus differ in network_names AND group_tins --
                               the same rates reached through provider groups
                               of different breadth, so genuinely two products.

    So the duplicate key is every identity column except network_names, and
    group_tins is what keeps the UnitedHealthcare pairs apart. Order-
    independent (a summed hash), because row order carries no meaning here.
    """
    sums = ", ".join("SUM(hash(%s))::VARCHAR" % c for c in COMPARED_COLUMNS)
    values = scalar(con, "SELECT %s FROM %s" % (sums, src))
    return dict(zip(COMPARED_COLUMNS, values))


def as_source(path):
    """A parquet path as a DuckDB table expression."""
    return "read_parquet('%s')" % path.replace("\\", "/")


def validate_file(path, payer, strict, expect_month, majority_month):
    con = duckdb.connect()
    src = as_source(path)
    f = Findings(strict)
    sig = None
    n, = scalar(con, "SELECT COUNT(*) FROM " + src)
    if n == 0:
        f.add(FAIL, "nonempty", "file has no rows")
    elif check_schema(con, src, f):
        check_join_keys(con, src, f)
        check_provider_identity(con, src, f)
        check_rates(con, src, f)
        check_constants(con, src, f, payer)
        check_blank_columns(con, src, f)
        check_month(con, src, f, expect_month, majority_month)
        sig = content_signature(con, src)
    con.close()
    return f, sig, n


def parse_args(argv):
    strict = "--strict" in argv
    quiet = "--quiet" in argv
    expect_month = None
    skip = set()
    if "--expect-month" in argv:
        i = argv.index("--expect-month")
        if i + 1 >= len(argv):
            raise SystemExit("--expect-month needs a value, e.g. 2026-08")
        expect_month = argv[i + 1]
        skip.add(i + 1)
    payers = [a for i, a in enumerate(argv)
              if not a.startswith("--") and i not in skip]
    return strict, quiet, expect_month, payers


def majority_reporting_month(paths):
    """
    The month most of the dataset was published for.

    Derived rather than configured so a stale file stands out against its
    siblings with no setup: whatever most files say is the baseline, and
    anything behind it gets flagged.
    """
    con = duckdb.connect()
    months = []
    for p in paths:
        try:
            m, = scalar(con, "SELECT ANY_VALUE(last_updated_on) FROM "
                        + as_source(p))
            if m:
                months.append(m[:7])
        except duckdb.Error:
            pass  # a malformed file is the per-file checks' problem
    con.close()
    return Counter(months).most_common(1)[0][0] if months else None


def main(argv=()):
    argv = list(argv)
    strict, quiet, expect_month, payers = parse_args(argv)

    paths = sorted(glob.glob(os.path.join(config.OUTPUT_DIR, "*.parquet")))
    if not paths:
        raise SystemExit("no parquet files in %s/ -- run run_pipeline.py first"
                         % config.OUTPUT_DIR)
    if payers:
        wanted = set(payers)
        paths = [p for p in paths if os.path.basename(p)[:-8] in wanted]
        found = {os.path.basename(p)[:-8] for p in paths}
        for miss in sorted(wanted - found):
            print("  no such parquet: " + miss)
        if not paths:
            raise SystemExit(1)

    # In-flight writes are .part until the atomic rename, so the glob above
    # never sees a partial file. Say what is still running rather than
    # silently validating a smaller dataset than the caller expects.
    inflight = sorted(os.path.basename(p)[:-13] for p in
                      glob.glob(os.path.join(config.OUTPUT_DIR, "*.part")))
    if inflight:
        print("note: still being written, not checked: %s\n"
              % ", ".join(inflight))

    # Establish the dataset's reporting month before checking any file, so a
    # single stale file stands out instead of every file being compared
    # against nothing.
    majority_month = None
    if not expect_month and len(paths) > 1:
        majority_month = majority_reporting_month(paths)

    fails = warns = total_rows = 0
    signatures = {}
    for path in paths:
        payer = os.path.basename(path)[:-8]
        f, sig, n = validate_file(path, payer, strict, expect_month,
                                  majority_month)
        total_rows += n
        fails += f.count(FAIL)
        warns += f.count(WARN)
        if sig:
            key = tuple(sig[c] for c in IDENTITY_COLUMNS)
            signatures.setdefault(key, []).append((payer, n, sig))

        bad = [i for i in f.items if i[0] != OK]
        if bad or not quiet:
            print("%s  (%s rows)" % (payer, f"{n:,}"))
            for level, check, msg in (bad if quiet else f.items):
                print("  %-4s %-18s %s" % (level, check, msg))
            print()

    for group in signatures.values():
        if len(group) > 1:
            warns += 1
            names = [p for p, _, _ in group]
            rows = group[0][1]
            first = group[0][2]
            varying = sorted(c for c in COMPARED_COLUMNS
                             if any(s[c] != first[c] for _, _, s in group))
            differs = ("differing only in %s" % ", ".join(varying)
                       if varying else "byte-identical in every column")
            print("WARN duplicate_files    %s hold the same %s rates, %s -- "
                  "one is the other under a second name; pick one, do not "
                  "union them\n" % (" and ".join(names), f"{rows:,}", differs))

    print("%d file(s), %s rows: %d failure(s), %d warning(s)"
          % (len(paths), f"{total_rows:,}", fails, warns))
    if fails or (strict and warns):
        raise SystemExit(1)


if __name__ == "__main__":
    main(sys.argv[1:])
