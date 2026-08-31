"""
Smoke tests for the MRF parser and the system-attribution logic.

No new dependencies: run with

    python -m unittest test_pipeline -v

The fixture is a tiny synthetic in-network file covering the cases that
actually bite in real payer data:

  * variant A  - rates that point at provider_references
  * variant B  - rates with inline provider_groups
  * a provider group holding NPIs from two different systems
  * a group with no target NPIs at all (must be dropped)
  * negotiated_value used in place of negotiated_rate
  * gzipped and plain files behind the same code path
"""

import gzip
import json
import os
import shutil
import tempfile
import unittest

import pandas as pd

from mrf_parser import build_relevant_groups, stream_filtered_rates, open_source
from run_pipeline import systems_for, cache_name, parse_one


# NPI -> system for the fixture. 9999999999 is deliberately NOT a target.
NPI_TO_SYSTEM = {
    "1111111111": "System A",
    "2222222222": "System B",
    "3333333333": "System A",
}
TARGET_NPIS = set(NPI_TO_SYSTEM)

FIXTURE = {
    "reporting_entity_name": "Test Payer",
    "version": "2.0.0",
    "provider_references": [
        {   # spans two systems -> the case the old code got wrong
            "provider_group_id": 1,
            "provider_groups": [{
                "npi": [1111111111, 2222222222],
                "tin": {"type": "ein", "value": "111111111"},
            }],
        },
        {   # no target NPIs -> must be dropped in pass 1
            "provider_group_id": 2,
            "provider_groups": [{
                "npi": [9999999999],
                "tin": {"type": "ein", "value": "999999999"},
            }],
        },
    ],
    "in_network": [
        {
            "billing_code": "27447",
            "billing_code_type": "CPT",
            "description": "TOTAL KNEE ARTHROPLASTY",
            "negotiated_rates": [
                {   # variant A, hits the multi-system group
                    "provider_references": [1],
                    "negotiated_prices": [{
                        "negotiated_rate": 1000.0,
                        "negotiated_type": "negotiated",
                        "billing_class": "institutional",
                        "service_code": ["21", "22"],
                        "expiration_date": "9999-12-31",
                    }],
                },
                {   # variant A, but the group has no targets -> excluded
                    "provider_references": [2],
                    "negotiated_prices": [{
                        "negotiated_rate": 500.0,
                        "negotiated_type": "negotiated",
                        "billing_class": "institutional",
                    }],
                },
            ],
        },
        {
            "billing_code": "99213",
            "billing_code_type": "CPT",
            "description": "OFFICE VISIT",
            "negotiated_rates": [
                {   # variant B inline, and negotiated_value not _rate
                    "provider_groups": [{
                        "npi": [3333333333],
                        "tin": {"type": "ein", "value": "333333333"},
                    }],
                    "negotiated_prices": [{
                        "negotiated_value": 75.0,
                        "negotiated_type": "negotiated",
                        "billing_class": "professional",
                    }],
                },
                {   # inline, non-target only -> excluded
                    "provider_groups": [{
                        "npi": [9999999999],
                        "tin": {"type": "ein", "value": "999999999"},
                    }],
                    "negotiated_prices": [{
                        "negotiated_value": 42.0,
                        "negotiated_type": "negotiated",
                        "billing_class": "professional",
                    }],
                },
            ],
        },
    ],
}


class FixtureCase(unittest.TestCase):
    """Writes the fixture to disk as both plain JSON and gzip."""

    @classmethod
    def setUpClass(cls):
        cls.tmp = tempfile.mkdtemp(prefix="mrf_test_")
        cls.plain = os.path.join(cls.tmp, "mrf.json")
        with open(cls.plain, "w", encoding="utf-8") as f:
            json.dump(FIXTURE, f)
        # same bytes, gzipped, with a name that does NOT end in .gz
        cls.gz = os.path.join(cls.tmp, "mrf_compressed.json")
        with gzip.open(cls.gz, "wb") as f:
            f.write(json.dumps(FIXTURE).encode("utf-8"))

    @classmethod
    def tearDownClass(cls):
        shutil.rmtree(cls.tmp, ignore_errors=True)


class TestPassOne(FixtureCase):

    def test_keeps_only_groups_with_target_npis(self):
        rel = build_relevant_groups(self.plain, TARGET_NPIS)
        self.assertEqual(set(rel), {"1"}, "group 2 has no targets, drop it")

    def test_records_only_matching_npis_and_the_tin(self):
        rel = build_relevant_groups(self.plain, TARGET_NPIS)
        self.assertEqual(rel["1"]["npis"], "1111111111,2222222222")
        self.assertEqual(rel["1"]["tins"], "111111111")


class TestPassTwo(FixtureCase):

    def rows(self, source):
        rel = build_relevant_groups(source, TARGET_NPIS)
        return list(stream_filtered_rates(source, rel, TARGET_NPIS))

    def test_emits_one_row_per_matching_price(self):
        rows = self.rows(self.plain)
        self.assertEqual(len(rows), 2, "the two non-target groups drop out")

    def test_variant_a_reference_row(self):
        row = next(r for r in self.rows(self.plain)
                   if r["billing_code"] == "27447")
        self.assertEqual(row["negotiated_rate"], 1000.0)
        self.assertEqual(row["matched_npis"], "1111111111,2222222222")
        self.assertEqual(row["billing_class"], "institutional")
        self.assertEqual(row["service_codes"], "21|22")

    def test_variant_b_inline_row_with_negotiated_value(self):
        row = next(r for r in self.rows(self.plain)
                   if r["billing_code"] == "99213")
        self.assertEqual(row["negotiated_rate"], 75.0,
                         "negotiated_value must be accepted as a fallback")
        self.assertEqual(row["matched_npis"], "3333333333")

    def test_non_target_rates_never_appear(self):
        rates = {r["negotiated_rate"] for r in self.rows(self.plain)}
        self.assertNotIn(500.0, rates)
        self.assertNotIn(42.0, rates)

    def test_max_records_caps_output(self):
        rel = build_relevant_groups(self.plain, TARGET_NPIS)
        capped = list(stream_filtered_rates(
            self.plain, rel, TARGET_NPIS, max_records=1))
        self.assertEqual(len(capped), 1)

    def test_gzip_is_detected_by_content_not_extension(self):
        self.assertEqual(self.rows(self.gz), self.rows(self.plain),
                         "a gzipped file named .json must parse identically")


class TestOpenSource(FixtureCase):

    def test_plain_file_round_trips(self):
        f, closer = open_source(self.plain)
        try:
            self.assertEqual(f.read(1), b"{")
        finally:
            closer()

    def test_gzip_file_is_transparently_decompressed(self):
        f, closer = open_source(self.gz)
        try:
            self.assertEqual(f.read(1), b"{")
        finally:
            closer()


class TestSystemAttribution(unittest.TestCase):
    """The bug that misattributed ~44% of real rows."""

    def test_multi_system_group_reports_every_system(self):
        self.assertEqual(
            systems_for("1111111111,2222222222", NPI_TO_SYSTEM),
            "System A,System B")

    def test_single_system_group(self):
        self.assertEqual(systems_for("3333333333", NPI_TO_SYSTEM), "System A")

    def test_two_npis_one_system_is_not_double_counted(self):
        self.assertEqual(
            systems_for("1111111111,3333333333", NPI_TO_SYSTEM), "System A")

    def test_unknown_npi_contributes_nothing(self):
        self.assertEqual(systems_for("9999999999", NPI_TO_SYSTEM), "")

    def test_result_is_sorted_and_stable(self):
        self.assertEqual(systems_for("2222222222,1111111111", NPI_TO_SYSTEM),
                         systems_for("1111111111,2222222222", NPI_TO_SYSTEM))


class TestCacheName(unittest.TestCase):
    """Signed payer URLs must not produce illegal Windows filenames."""

    ILLEGAL = set(['<', '>', ':', '"', '/', '\\', '|', '?', '*'])

    def test_query_string_is_stripped(self):
        got = cache_name(
            "https://x.blob.core.windows.net/a/in-network.json.gz"
            "?sv=2021&sig=ab%2Fcd")
        self.assertEqual(got, "in-network.json.gz")

    def test_no_illegal_characters_survive(self):
        got = cache_name("https://x.com/a/we:ird*name?q=1")
        self.assertFalse(self.ILLEGAL & set(got), f"illegal chars in {got!r}")

    def test_never_returns_empty(self):
        self.assertTrue(cache_name("https://x.com/"))


class TestParseOneEndToEnd(FixtureCase):

    def test_writes_parquet_with_attribution_columns(self):
        out = os.path.join(self.tmp, "Test.parquet")
        n = parse_one("Test", self.plain, TARGET_NPIS, NPI_TO_SYSTEM, out)
        self.assertEqual(n, 2)

        df = pd.read_parquet(out)
        self.assertIn("systems", df.columns)
        self.assertIn("system_count", df.columns)
        self.assertNotIn("system", df.columns,
                         "the old single-system column must be gone")

        knee = df[df["billing_code"] == "27447"].iloc[0]
        self.assertEqual(knee["systems"], "System A,System B")
        self.assertEqual(knee["system_count"], 2)

        visit = df[df["billing_code"] == "99213"].iloc[0]
        self.assertEqual(visit["systems"], "System A")
        self.assertEqual(visit["system_count"], 1)


if __name__ == "__main__":
    unittest.main(verbosity=2)
