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

import contextlib
import gzip
import io
import json
import os
import shutil
import tempfile
import unittest
import urllib.error
from unittest import mock

import pandas as pd
import requests

import config
import validate
from mrf_parser import (build_relevant_groups, stream_filtered_rates,
                        open_source, read_header, _network_names,
                        is_transient, retry, TruncatedDownload, BACKOFF_CAP)
from run_pipeline import (load_target_tins, systems_for, cache_name,
                          parse_one, download_once)
from find_files import (scan_index, matches, is_ancillary,
                        parse_args as find_files_args)


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


class TestZipRejection(unittest.TestCase):
    """A few payer rate files are .zip; fail with an explanation, not a
    confusing gzip/JSON parse error deep in pass 1."""

    def test_zip_file_raises_a_useful_error(self):
        tmp = tempfile.mkdtemp(prefix="mrf_zip_")
        try:
            import zipfile
            p = os.path.join(tmp, "rates.zip")
            with zipfile.ZipFile(p, "w") as z:
                z.writestr("rates.json", '{"in_network": []}')
            with self.assertRaises(ValueError) as cm:
                open_source(p)
            self.assertIn("ZIP archive", str(cm.exception))
            self.assertIn("find_files.py", str(cm.exception))
        finally:
            shutil.rmtree(tmp, ignore_errors=True)


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


class TestTinMatching(FixtureCase):
    """
    Group 2 / NPI 9999999999 is not a target NPI, so NPI matching drops it.
    Its tax ID 999999999 as a target TIN must bring it back -- in both the
    referenced (variant A) and inline (variant B) shapes -- and attribute
    it to the TIN's system.
    """
    TINS = frozenset({"999999999"})
    TIN_TO_SYSTEM = {"999999999": "System C"}

    def test_pass_one_keeps_group_by_tin(self):
        rel = build_relevant_groups(self.plain, TARGET_NPIS, self.TINS)
        self.assertIn("2", rel)
        self.assertEqual(rel["2"]["matched_tins"], "999999999")
        self.assertEqual(rel["2"]["npis"], "")        # no NPI hit, TIN only
        self.assertEqual(rel["1"]["matched_tins"], "")  # NPI hit, no TIN

    def test_pass_one_records_group_sizes_for_every_group(self):
        sizes = {}
        build_relevant_groups(self.plain, TARGET_NPIS, group_sizes=sizes)
        self.assertEqual(sizes, {"1": 1, "2": 1})

    def test_pass_two_emits_tin_matched_rows_both_variants(self):
        sizes = {}
        rel = build_relevant_groups(self.plain, TARGET_NPIS, self.TINS,
                                    group_sizes=sizes)
        rows = list(stream_filtered_rates(self.plain, rel, TARGET_NPIS,
                                          self.TINS, group_sizes=sizes))
        rates = sorted(r["negotiated_rate"] for r in rows)
        self.assertEqual(rates, [42.0, 75.0, 500.0, 1000.0])
        by_rate = {r["negotiated_rate"]: r for r in rows}
        self.assertEqual(by_rate[500.0]["matched_tins"], "999999999")   # A
        self.assertEqual(by_rate[500.0]["matched_npis"], "")
        self.assertEqual(by_rate[42.0]["matched_tins"], "999999999")    # B
        self.assertEqual(by_rate[1000.0]["matched_tins"], "")
        for r in rows:
            self.assertEqual(r["group_tins"], 1)

    def test_no_target_tins_is_unchanged_behaviour(self):
        rel = build_relevant_groups(self.plain, TARGET_NPIS)
        rows = list(stream_filtered_rates(self.plain, rel, TARGET_NPIS))
        self.assertEqual(sorted(r["negotiated_rate"] for r in rows),
                         [75.0, 1000.0])

    def test_attribution_unions_npi_and_tin_systems(self):
        self.assertEqual(
            systems_for("1111111111", NPI_TO_SYSTEM, "999999999",
                        self.TIN_TO_SYSTEM),
            "System A,System C")
        self.assertEqual(
            systems_for("", NPI_TO_SYSTEM, "999999999", self.TIN_TO_SYSTEM),
            "System C")
        self.assertEqual(systems_for("", NPI_TO_SYSTEM, "", None), "")

    def test_load_target_tins_reads_only_include_y(self):
        path = os.path.join(self.tmp, "tins.csv")
        with open(path, "w", encoding="utf-8", newline="") as f:
            f.write("include,system,tin,entity\n"
                    "Y,System C,999999999,Hosp C\n"
                    "?,System D,888888888,maybe\n"
                    "N,System E,777777777,no\n"
                    "y ,System F, 666666666 ,lowercase and spaces\n")
        self.assertEqual(load_target_tins(path),
                         {"999999999": "System C", "666666666": "System F"})
        self.assertEqual(load_target_tins(os.path.join(self.tmp, "nope.csv")),
                         {})


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
        self.assertFalse(os.path.exists(out + ".part"))

    def test_tin_matching_adds_rows_and_attributes_by_tin(self):
        out = os.path.join(self.tmp, "TestTin.parquet")
        n = parse_one("TestTin", self.plain, TARGET_NPIS, NPI_TO_SYSTEM, out,
                      tin_to_system={"999999999": "System C"})
        self.assertEqual(n, 4)
        df = pd.read_parquet(out)
        c = df[df["matched_tins"] == "999999999"]
        self.assertEqual(len(c), 2)
        self.assertTrue((c["systems"] == "System C").all())
        self.assertNotIn("tins", df.columns,
                         "the full per-row TIN list is gone (memory)")
        self.assertIn("group_tins", df.columns)

    def test_chunked_writes_produce_one_complete_file(self):
        import run_pipeline
        out = os.path.join(self.tmp, "TestChunk.parquet")
        old = run_pipeline.CHUNK_ROWS
        run_pipeline.CHUNK_ROWS = 1  # force a flush per row
        try:
            n = parse_one("TestChunk", self.plain, TARGET_NPIS,
                          NPI_TO_SYSTEM, out)
        finally:
            run_pipeline.CHUNK_ROWS = old
        self.assertEqual(n, 2)
        df = pd.read_parquet(out)
        self.assertEqual(len(df), 2)
        self.assertEqual(sorted(df["billing_code"]), ["27447", "99213"])


INDEX_FIXTURE = {
    "reporting_entity_name": "Aetna Life Insurance Company",
    "reporting_structure": [
        {   # broad employer PPO, referenced by two structures
            "reporting_plans": [
                {"plan_name": "Aetna Choice POS II NY",
                 "plan_market_type": "group"},
                {"plan_name": "Aetna Open Access PPO NY",
                 "plan_market_type": "group"},
            ],
            "in_network_files": [{
                "description": "in-network",
                "location": "https://x.com/a/broad_ppo_in-network.json.gz?sig=a",
            }],
        },
        {
            "reporting_plans": [
                {"plan_name": "Aetna Choice POS II NJ",
                 "plan_market_type": "group"},
            ],
            "in_network_files": [{
                "description": "in-network",
                "location": "https://x.com/a/broad_ppo_in-network.json.gz?sig=a",
            }],
        },
        {   # exchange plan -> the narrow network that caused 13% coverage
            "reporting_plans": [
                {"plan_name": "Aetna Exchange_Elect Choice_3082",
                 "plan_market_type": "individual"},
            ],
            "in_network_files": [{
                "description": "in-network",
                "location": "https://x.com/a/exchange_narrow_in-network.json.gz",
            }],
        },
    ],
}


class TestIndexScan(unittest.TestCase):
    """Choosing broad-network rate files out of a Table of Contents."""

    @classmethod
    def setUpClass(cls):
        cls.tmp = tempfile.mkdtemp(prefix="mrf_idx_")
        cls.path = os.path.join(cls.tmp, "index.json")
        with open(cls.path, "w", encoding="utf-8") as f:
            json.dump(INDEX_FIXTURE, f)
        # gzipped, deliberately NOT named .gz
        cls.gz = os.path.join(cls.tmp, "index_gz.json")
        with gzip.open(cls.gz, "wb") as f:
            f.write(json.dumps(INDEX_FIXTURE).encode("utf-8"))

    @classmethod
    def tearDownClass(cls):
        shutil.rmtree(cls.tmp, ignore_errors=True)

    def test_deduplicates_files_shared_by_several_structures(self):
        files, seen = scan_index(self.path, progress_every=0)
        self.assertEqual(seen, 3)
        self.assertEqual(len(files), 2, "the PPO file appears twice, count once")

    def test_counts_every_plan_referencing_a_file(self):
        files, _ = scan_index(self.path, progress_every=0)
        ppo = files["https://x.com/a/broad_ppo_in-network.json.gz?sig=a"]
        self.assertEqual(ppo["plans"], 3, "2 plans + 1 plan across 2 structures")

    def test_market_type_is_collected(self):
        files, _ = scan_index(self.path, progress_every=0)
        ppo = files["https://x.com/a/broad_ppo_in-network.json.gz?sig=a"]
        ex = files["https://x.com/a/exchange_narrow_in-network.json.gz"]
        self.assertEqual(ppo["markets"], {"group"})
        self.assertEqual(ex["markets"], {"individual"})

    def test_group_filter_excludes_exchange_files(self):
        files, _ = scan_index(self.path, progress_every=0)
        kept = [loc for loc, rec in files.items()
                if matches(loc, rec, "group", None, [])]
        self.assertEqual(kept,
                         ["https://x.com/a/broad_ppo_in-network.json.gz?sig=a"])

    def test_contains_filter_matches_plan_names(self):
        files, _ = scan_index(self.path, progress_every=0)
        kept = [loc for loc, rec in files.items()
                if matches(loc, rec, "any", None, ["exchange"])]
        self.assertEqual(kept,
                         ["https://x.com/a/exchange_narrow_in-network.json.gz"])

    def test_gzipped_index_named_json_still_scans(self):
        a, _ = scan_index(self.path, progress_every=0)
        b, _ = scan_index(self.gz, progress_every=0)
        self.assertEqual(set(a), set(b))

    def test_args_require_a_source(self):
        with self.assertRaises(SystemExit):
            find_files_args([])

    def test_args_reject_a_bad_market(self):
        with self.assertRaises(SystemExit):
            find_files_args(["idx.json", "--market", "bogus"])

    def test_contains_collects_multiple_keywords(self):
        _, market, _, _, _, _, _, contains = find_files_args(
            ["idx.json", "--contains", "PPO", "Open Access", "--market", "group"])
        self.assertEqual(contains, ["ppo", "open access"])
        self.assertEqual(market, "group")

    def test_medical_flag_defaults_off(self):
        *_, medical, _ = find_files_args(["idx.json"])
        self.assertFalse(medical)
        *_, medical, _ = find_files_args(["idx.json", "--medical"])
        self.assertTrue(medical)

    def test_id_type_parses_and_validates(self):
        _, _, _, idt, *_ = find_files_args(["idx.json"])
        self.assertEqual(idt, "any")
        _, _, _, idt, *_ = find_files_args(["idx.json", "--id-type", "ein"])
        self.assertEqual(idt, "ein")
        with self.assertRaises(SystemExit):
            find_files_args(["idx.json", "--id-type", "bogus"])

    def test_id_type_filter_separates_employer_from_exchange(self):
        """Aetna labels exchange plans plan_market_type "group", so the id
        type is the only thing that tells them apart."""
        ein = {"markets": {"group"}, "id_types": {"ein"},
               "description": "", "names": []}
        hios = {"markets": {"group"}, "id_types": {"hios"},
                "description": "", "names": []}
        loc = "https://x.com/a/rates.json.gz"
        self.assertTrue(matches(loc, ein, "group", None, [], id_type="ein"))
        self.assertFalse(matches(loc, hios, "group", None, [], id_type="ein"))
        # both still pass the market filter alone -- which is the trap
        self.assertTrue(matches(loc, hios, "group", None, []))


class TestAncillaryFilter(unittest.TestCase):
    """Dental/vision/behavioral files are referenced by every plan, so plan
    count alone floats them above the medical networks that matter."""

    def test_ancillary_names_are_detected(self):
        for name in ("UHC---Embedded-Dental_UHC-Dental_in-network-rates.json.gz",
                     "UHC---Embedded-Vision_UHC-Vision_in-network-rates.json.gz",
                     "OHPH-Chiro_28_in-network-rates.json.gz",
                     "Optum-Health-Behavioral-Services--OHBS-_5_in-network.json.gz",
                     "CMC_Transplant_MRRF_in-network-rates.json.gz"):
            self.assertTrue(is_ancillary("https://x.com/a/" + name), name)

    def test_medical_networks_are_kept(self):
        for name in ("Choice-Plus_8_in-network-rates.json.gz",
                     "PP1-00_P3_in-network-rates.json.gz",
                     "PS1-50_C2_in-network-rates.json.gz",
                     "broad_ppo_in-network.json.gz"):
            self.assertFalse(is_ancillary("https://x.com/a/" + name), name)

    def test_matches_drops_ancillary_only_when_medical_set(self):
        loc = "https://x.com/a/UHC-Dental_in-network-rates.json.gz"
        rec = {"markets": {"group"}, "description": "", "names": []}
        self.assertTrue(matches(loc, rec, "any", None, [], medical=False))
        self.assertFalse(matches(loc, rec, "any", None, [], medical=True))


META_FIXTURE = {
    "reporting_entity_name": "Meta Payer",
    "reporting_entity_type": "Health Insurance Issuer",
    "last_updated_on": "2026-08-05",
    "version": "2.0.1",
    "provider_references": [
        {   # Aetna shape: one group in several networks at once
            "provider_group_id": 1,
            "network_name": ["Open Access Elect Choice", "Aetna Select"],
            "provider_groups": [{
                "npi": [1111111111],
                "tin": {"type": "ein", "value": "111111111",
                        "business_name": "Test Hospital;Test Clinic"},
            }],
        },
    ],
    "in_network": [
        {
            "billing_code": "27447",
            "billing_code_type": "CPT",
            "description": "TOTAL KNEE ARTHROPLASTY",
            "negotiated_rates": [{
                "provider_references": [1],
                "negotiated_prices": [{
                    "negotiated_rate": 1000.0,
                    "negotiated_type": "negotiated",
                    "billing_class": "institutional",
                }],
            }],
        },
    ],
}


class TestNetworkNames(unittest.TestCase):
    """The network_name field is an array in the schema and Aetna uses it."""

    def test_array_is_read(self):
        self.assertEqual(_network_names({"network_name": ["B", "A"]}),
                         {"A", "B"})

    def test_bare_string_is_tolerated(self):
        self.assertEqual(_network_names({"network_name": "Solo"}), {"Solo"})

    def test_missing_and_null_are_empty(self):
        self.assertEqual(_network_names({}), set())
        self.assertEqual(_network_names({"network_name": None}), set())

    def test_pipe_in_a_name_cannot_corrupt_the_join(self):
        # '|' is the separator, so it must not survive inside a value
        self.assertEqual(_network_names({"network_name": ["A|B"]}), {"A/B"})


class TestHeaderAndMetadata(unittest.TestCase):
    """read_header, and the metadata pass 1 now collects."""

    @classmethod
    def setUpClass(cls):
        cls.tmp = tempfile.mkdtemp(prefix="mrf_meta_")
        cls.compact = os.path.join(cls.tmp, "compact.json")
        with open(cls.compact, "w", encoding="utf-8") as f:
            json.dump(META_FIXTURE, f, separators=(",", ":"))
        # UnitedHealthcare pretty-prints; the regex must survive the spaces
        cls.pretty = os.path.join(cls.tmp, "pretty.json")
        with open(cls.pretty, "w", encoding="utf-8") as f:
            json.dump(META_FIXTURE, f, indent=4)
        cls.gz = os.path.join(cls.tmp, "compact_gz.json")
        with gzip.open(cls.gz, "wb") as f:
            f.write(json.dumps(META_FIXTURE).encode("utf-8"))

    @classmethod
    def tearDownClass(cls):
        shutil.rmtree(cls.tmp, ignore_errors=True)

    def test_reads_compact_pretty_and_gzip_alike(self):
        for path in (self.compact, self.pretty, self.gz):
            h = read_header(path)
            self.assertEqual(h["reporting_entity_name"], "Meta Payer", path)
            self.assertEqual(h["last_updated_on"], "2026-08-05", path)
            self.assertEqual(h["version"], "2.0.1", path)

    def test_absent_field_is_empty_not_missing(self):
        path = os.path.join(self.tmp, "bare.json")
        with open(path, "w", encoding="utf-8") as f:
            json.dump({"in_network": []}, f)
        self.assertEqual(read_header(path)["last_updated_on"], "")

    def test_pass_one_collects_networks_and_business_names(self):
        names = {}
        rel = build_relevant_groups(self.compact, {"1111111111"},
                                    frozenset({"111111111"}), tin_names=names)
        self.assertEqual(rel["1"]["networks"],
                         "Aetna Select|Open Access Elect Choice")
        self.assertEqual(names, {"111111111": "Test Hospital;Test Clinic"})

    def test_pass_one_records_names_only_for_matched_tins(self):
        names = {}
        build_relevant_groups(self.compact, {"1111111111"}, frozenset(),
                              tin_names=names)
        self.assertEqual(names, {})   # matched by NPI, no TIN hit

    def test_rate_rows_carry_the_network(self):
        names = {}
        rel = build_relevant_groups(self.compact, {"1111111111"},
                                    frozenset(), tin_names=names)
        rows = list(stream_filtered_rates(self.compact, rel,
                                          {"1111111111"}, frozenset()))
        self.assertEqual(len(rows), 1)
        self.assertEqual(rows[0]["network_names"],
                         "Aetna Select|Open Access Elect Choice")


class TestRetryPolicy(unittest.TestCase):
    """
    Which failures earn another attempt, and how many.

    The distinction that matters: a 503 from an overloaded CDN is worth
    waiting out, an expired signed URL's 403 is not, and retrying the latter
    only turns a clear error into a slow one.
    """

    @staticmethod
    def http_error(code):
        return urllib.error.HTTPError("http://x/f.gz", code, "msg", {}, None)

    def test_overload_and_throttling_are_transient(self):
        for code in (408, 429, 500, 502, 503, 504):
            self.assertTrue(is_transient(self.http_error(code)), code)

    def test_permanent_http_errors_are_not(self):
        for code in (400, 401, 403, 404, 410):
            self.assertFalse(is_transient(self.http_error(code)), code)

    def test_socket_level_failures_are_transient(self):
        self.assertTrue(is_transient(requests.exceptions.ConnectionError()))
        self.assertTrue(is_transient(requests.exceptions.Timeout()))
        self.assertTrue(is_transient(ConnectionResetError()))
        self.assertTrue(is_transient(TruncatedDownload("short")))

    def test_a_bug_is_not_transient(self):
        self.assertFalse(is_transient(ValueError("bad json")))
        self.assertFalse(is_transient(KeyError("missing")))

    def test_succeeds_after_transient_failures(self):
        calls, sleeps = [], []

        def flaky():
            calls.append(1)
            if len(calls) < 3:
                raise requests.exceptions.ConnectionError("reset")
            return "done"

        self.assertEqual(
            retry(flaky, "fetch", log=None, sleep=sleeps.append), "done")
        self.assertEqual(len(calls), 3)
        self.assertEqual(sleeps, [2.0, 4.0])      # exponential, not flat

    def test_gives_up_and_reraises(self):
        calls = []

        def always():
            calls.append(1)
            raise requests.exceptions.Timeout("slow")

        with self.assertRaises(requests.exceptions.Timeout):
            retry(always, "fetch", attempts=3, log=None, sleep=lambda s: None)
        self.assertEqual(len(calls), 3)

    def test_permanent_failure_is_not_slept_on(self):
        calls, sleeps = [], []

        def bad():
            calls.append(1)
            raise self.http_error(404)

        with self.assertRaises(urllib.error.HTTPError):
            retry(bad, "fetch", log=None, sleep=sleeps.append)
        self.assertEqual(len(calls), 1)   # no second attempt
        self.assertEqual(sleeps, [])

    def test_backoff_is_capped(self):
        sleeps = []

        def always():
            raise requests.exceptions.ConnectionError("reset")

        with self.assertRaises(requests.exceptions.ConnectionError):
            retry(always, "fetch", attempts=8, log=None, sleep=sleeps.append)
        self.assertTrue(max(sleeps) <= BACKOFF_CAP, sleeps)


class FakeResponse:
    """Minimal stand-in for a urlopen result."""

    def __init__(self, body, status=200, claim_length=None):
        self._body = body
        self._pos = 0
        self.status = status
        length = len(body) if claim_length is None else claim_length
        self.headers = {"Content-Length": str(length)}

    def read(self, n):
        chunk = self._body[self._pos:self._pos + n]
        self._pos += len(chunk)
        return chunk

    def getcode(self):
        return self.status

    def __enter__(self):
        return self

    def __exit__(self, *exc):
        return False


BODY = b"0123456789abcdef"


class TestResumableDownload(unittest.TestCase):
    """
    A dropped connection must not cost the bytes already on disk.

    These files reach 15 GB. Restarting from zero at 12 GB was the old
    behaviour and it is the difference between a two-minute recovery and an
    hour of re-downloading.
    """

    def setUp(self):
        self.tmp = tempfile.mkdtemp(prefix="mrf_dl_")
        self.url = "https://payer.example/mrf/rates.json.gz"
        self.local = os.path.join(self.tmp, cache_name(self.url))
        self.part = self.local + ".part"
        self.requests_seen = []

    def tearDown(self):
        shutil.rmtree(self.tmp, ignore_errors=True)

    def run_download(self, responses):
        """Patch urlopen to return `responses` in order; return the bytes."""
        pending = list(responses)

        def fake_urlopen(req, timeout=None):
            self.requests_seen.append(req.headers.get("Range"))
            return pending.pop(0)

        with mock.patch("urllib.request.urlopen", fake_urlopen), \
                mock.patch("mrf_parser.time.sleep"):
            path = download_once(self.url, self.tmp)
        with open(path, "rb") as f:
            return f.read()

    def test_plain_download_writes_the_whole_file(self):
        self.assertEqual(self.run_download([FakeResponse(BODY)]), BODY)
        self.assertEqual(self.requests_seen, [None])   # no Range on a fresh get
        self.assertFalse(os.path.exists(self.part))    # renamed, not left behind

    def test_resumes_from_a_partial_file(self):
        with open(self.part, "wb") as f:
            f.write(BODY[:6])
        got = self.run_download([FakeResponse(BODY[6:], status=206)])
        self.assertEqual(got, BODY)
        self.assertEqual(self.requests_seen, ["bytes=6-"])

    def test_server_ignoring_range_restarts_cleanly(self):
        # A 200 to a ranged request means the whole file is coming again, so
        # appending it to the partial would produce a corrupt double-length
        # file. It must truncate instead.
        with open(self.part, "wb") as f:
            f.write(BODY[:6])
        got = self.run_download([FakeResponse(BODY, status=200)])
        self.assertEqual(got, BODY)

    def test_truncated_download_is_retried_and_resumed(self):
        short = FakeResponse(BODY[:6], status=200, claim_length=len(BODY))
        rest = FakeResponse(BODY[6:], status=206)
        self.assertEqual(self.run_download([short, rest]), BODY)
        self.assertEqual(self.requests_seen, [None, "bytes=6-"])

    def test_a_cached_file_is_not_downloaded_again(self):
        with open(self.local, "wb") as f:
            f.write(BODY)

        def explode(req, timeout=None):
            raise AssertionError("should not have hit the network")

        with mock.patch("urllib.request.urlopen", explode):
            self.assertEqual(download_once(self.url, self.tmp), self.local)


def validation_frame(**override):
    """Three clean rows carrying every column of the contract."""
    df = pd.DataFrame({
        "billing_code": ["27447", "99213", "27447"],
        "code_type": ["CPT"] * 3,
        "description": ["KNEE", "OFFICE VISIT", "KNEE"],
        "negotiated_rate": [1000.0, 120.0, 1500.0],
        "rate_type": ["negotiated"] * 3,
        "billing_class": ["institutional"] * 3,
        "service_codes": ["21", "11", "21"],
        "expiration_date": ["9999-12-31"] * 3,
        "matched_npis": ["1111111111", "", "1111111111"],
        "matched_tins": ["111111111"] * 3,
        "group_tins": [1, 4, 1],
        "network_names": ["Net A"] * 3,
        "payer": ["TestPayer"] * 3,
        "reporting_entity_name": ["Test Co"] * 3,
        "last_updated_on": ["2026-08-05"] * 3,
        "schema_version": ["2.0.0"] * 3,
        "systems": ["SystemOne"] * 3,
        "system_count": [1, 1, 1],
    })
    for col, values in override.items():
        df[col] = values
    return df


class ValidationCase(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.mkdtemp(prefix="mrf_val_")

    def tearDown(self):
        shutil.rmtree(self.tmp, ignore_errors=True)

    def write(self, payer, df):
        path = os.path.join(self.tmp, payer + ".parquet")
        df.to_parquet(path, index=False)
        return path

    def findings(self, df, payer="TestPayer", expect_month=None,
                 majority_month=None):
        path = self.write(payer, df)
        f, _, _ = validate.validate_file(path, payer, False, expect_month,
                                         majority_month)
        return f

    def assertFails(self, f, check):
        hits = [c for lvl, c, _ in f.items if lvl == validate.FAIL]
        self.assertIn(check, hits, f.items)

    def assertWarns(self, f, check):
        hits = [c for lvl, c, _ in f.items if lvl == validate.WARN]
        self.assertIn(check, hits, f.items)


class TestValidateFile(ValidationCase):
    """The per-file checks in validate.py."""

    def test_a_clean_file_produces_nothing(self):
        f = self.findings(validation_frame())
        self.assertEqual(f.count(validate.FAIL), 0, f.items)
        self.assertEqual(f.count(validate.WARN), 0, f.items)

    def test_missing_column_fails(self):
        f = self.findings(validation_frame().drop(columns=["network_names"]))
        self.assertFails(f, "schema")

    def test_unexpected_column_fails(self):
        df = validation_frame()
        df["surprise"] = 1
        self.assertFails(self.findings(df), "schema")

    def test_wrong_type_fails(self):
        # group_tins as a float would silently break any integer comparison
        f = self.findings(validation_frame(group_tins=[1.0, 4.0, 1.0]))
        self.assertFails(f, "schema")

    def test_blank_join_key_fails(self):
        f = self.findings(validation_frame(billing_code=["27447", "", "27447"]))
        self.assertFails(f, "join_keys")

    def test_row_naming_no_provider_fails(self):
        f = self.findings(validation_frame(
            matched_npis=["1111111111", "", "1111111111"],
            matched_tins=["111111111", "", "111111111"]))
        self.assertFails(f, "provider_identity")

    def test_null_and_negative_rates_fail(self):
        self.assertFails(
            self.findings(validation_frame(
                negotiated_rate=[1000.0, None, 1500.0])), "rates")
        self.assertFails(
            self.findings(validation_frame(
                negotiated_rate=[1000.0, -5.0, 1500.0])), "rates")

    def test_mostly_zero_rates_warn(self):
        f = self.findings(validation_frame(negotiated_rate=[0.0, 0.0, 0.0]))
        self.assertWarns(f, "rates")

    def test_all_blank_required_column_fails(self):
        # This is the network_names class of bug, on a column that must
        # never be empty.
        f = self.findings(validation_frame(systems=["", "", ""]))
        self.assertFails(f, "blank_column")

    def test_all_blank_optional_column_only_warns(self):
        # network_names is legitimately empty on files parsed before the
        # column existed, so it must not fail a run that is otherwise fine.
        f = self.findings(validation_frame(network_names=["", "", ""]))
        self.assertWarns(f, "blank_column")
        self.assertEqual(f.count(validate.FAIL), 0, f.items)

    def test_a_column_blank_on_some_rows_is_fine(self):
        f = self.findings(validation_frame())   # matched_npis blank on row 2
        self.assertEqual(f.count(validate.WARN), 0, f.items)

    def test_payer_not_matching_the_filename_fails(self):
        f = self.findings(validation_frame(payer=["Other"] * 3))
        self.assertFails(f, "constants")

    def test_two_reporting_dates_in_one_file_fails(self):
        f = self.findings(validation_frame(
            last_updated_on=["2026-08-05", "2026-06-05", "2026-08-05"]))
        self.assertFails(f, "constants")

    def test_wrong_expected_month_fails(self):
        f = self.findings(validation_frame(), expect_month="2026-09")
        self.assertFails(f, "as_of")

    def test_right_expected_month_passes(self):
        f = self.findings(validation_frame(), expect_month="2026-08")
        self.assertEqual(f.count(validate.FAIL), 0, f.items)

    def test_file_behind_its_siblings_warns(self):
        # Aetna_NY published 2026-06 while the rest of the dataset was
        # 2026-08; the rates look valid and are simply two months old.
        f = self.findings(validation_frame(), majority_month="2026-09")
        self.assertWarns(f, "as_of")

    def test_empty_file_fails(self):
        f = self.findings(validation_frame().iloc[0:0])
        self.assertFails(f, "nonempty")


class TestDuplicateDetection(ValidationCase):
    """
    Telling a republished file apart from a genuinely different product.

    Measured on the real dataset: Cigna's National and Pathwell files differ
    in network_names alone, while UnitedHealthcare's ChoicePlus and ChoiceEPO
    share a row count and a rate total but differ in group_tins. Only the
    first pair is a duplicate.
    """

    def run_main(self, *args):
        out = io.StringIO()
        with mock.patch.object(config, "OUTPUT_DIR", self.tmp), \
                contextlib.redirect_stdout(out):
            try:
                validate.main(list(args))
            except SystemExit:
                pass
        return out.getvalue()

    def test_same_rates_under_two_network_labels_is_flagged(self):
        self.write("PayerA", validation_frame(payer=["PayerA"] * 3))
        self.write("PayerB", validation_frame(payer=["PayerB"] * 3,
                                              network_names=["Net B"] * 3))
        out = self.run_main("--quiet")
        self.assertIn("duplicate_files", out)
        self.assertIn("differing only in network_names", out)

    def test_different_group_breadth_is_not_a_duplicate(self):
        self.write("PayerA", validation_frame(payer=["PayerA"] * 3))
        self.write("PayerB", validation_frame(payer=["PayerB"] * 3,
                                              network_names=["Net B"] * 3,
                                              group_tins=[9, 9, 9]))
        self.assertNotIn("duplicate_files", self.run_main("--quiet"))

    def test_strict_turns_warnings_into_a_failing_exit(self):
        self.write("PayerA", validation_frame(payer=["PayerA"] * 3,
                                              network_names=["", "", ""]))
        with mock.patch.object(config, "OUTPUT_DIR", self.tmp), \
                contextlib.redirect_stdout(io.StringIO()):
            with self.assertRaises(SystemExit):
                validate.main(["--strict"])

    def test_a_failure_exits_non_zero(self):
        self.write("PayerA", validation_frame(payer=["PayerA"] * 3,
                                              systems=["", "", ""]))
        with mock.patch.object(config, "OUTPUT_DIR", self.tmp), \
                contextlib.redirect_stdout(io.StringIO()):
            with self.assertRaises(SystemExit):
                validate.main([])

    def test_clean_dataset_exits_zero(self):
        self.write("PayerA", validation_frame(payer=["PayerA"] * 3))
        with mock.patch.object(config, "OUTPUT_DIR", self.tmp), \
                contextlib.redirect_stdout(io.StringIO()):
            validate.main([])      # no SystemExit


if __name__ == "__main__":
    unittest.main(verbosity=2)
