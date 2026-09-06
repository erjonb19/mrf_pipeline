"""
Core MRF parsing logic.

Two-pass, NPI-filtered parser for CMS Transparency-in-Coverage in-network
machine-readable files (schema 2.0). Handles both the reference-based and
inline provider-group structures, and the schema-2.0 'npi' (singular array)
field name.

Pass 1: stream provider_references, keep only groups containing a target NPI.
Pass 2: stream in_network, emit only rate rows touching a target provider.

Designed to read from a local .json, local .json.gz, or remote .json.gz URL.
"""

import gzip
import io
import re

import requests

# Prefer the fast C backend; fall back to pure-Python ijson if unavailable.
import ijson.common as ijson_common

try:
    import ijson.backends.yajl2_c as ijson
    BACKEND = "yajl2_c"
except Exception:  # pragma: no cover
    import ijson
    BACKEND = "python"


def _events_until_end_of(events, key):
    """
    Yield parse events, stopping as soon as top-level `key`'s array closes.

    ijson.items() keeps consuming the stream to EOF even after the array it
    is watching has ended, so reading provider_references off the front of a
    file used to drag the whole in_network block through the parser as well.
    On a 1.59 GB Cigna file that was 20+ minutes of wasted work per pass;
    stopping at the closing bracket makes it 21 seconds.
    """
    for prefix, event, value in events:
        yield prefix, event, value
        if prefix == key and event == "end_array":
            return


class _ChunkedStream(io.RawIOBase):
    """Wrap a streaming requests response so ijson can call .read()."""

    def __init__(self, response, chunk_size=65536):
        self._iter = response.iter_content(chunk_size=chunk_size)
        self._buf = b""

    def readinto(self, b):
        while not self._buf:
            try:
                self._buf = next(self._iter)
            except StopIteration:
                return 0
        n = min(len(b), len(self._buf))
        b[:n] = self._buf[:n]
        self._buf = self._buf[n:]
        return n

    def readable(self):
        return True


GZIP_MAGIC = bytes((0x1F, 0x8B))
ZIP_MAGIC = bytes((0x50, 0x4B))   # "PK"

_ZIP_MSG = (
    "{} is a ZIP archive, not JSON or gzip. Some payers publish a few rate "
    "files as .zip; this parser reads .json and .json.gz. Unzip it and point "
    "config at the extracted .json, or pick a .json.gz file instead "
    "(find_files.py lists the format of every candidate)."
)

# Several payer CDNs (UnitedHealthcare's among them) answer the default
# python-requests user agent with 403, and some reject HEAD outright. Present
# a browser UA and only ever GET.
HTTP_HEADERS = {
    "Accept-Encoding": "identity",
    "Accept": "*/*",
    "User-Agent": ("Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                   "AppleWebKit/537.36 (KHTML, like Gecko) "
                   "Chrome/128.0.0.0 Safari/537.36"),
}


def open_source(path_or_url):
    """
    Open a local or remote MRF uniformly, gzipped or not.

    Compression is detected from the first two bytes rather than from the
    file extension: payers serve plain .json from .gz URLs and vice versa,
    and signed URLs carry query strings that hide the real suffix.

    Returns (file_like, closer) where closer is a callable to release the
    underlying HTTP connection (or close the local file).
    """
    if path_or_url.startswith("http"):
        r = requests.get(
            path_or_url,
            stream=True,
            timeout=(10, 180),
            headers=HTTP_HEADERS,
        )
        r.raise_for_status()
        buf = io.BufferedReader(_ChunkedStream(r), buffer_size=262144)
        head = buf.peek(2)[:2]
        if head == ZIP_MAGIC:
            r.close()
            raise ValueError(_ZIP_MSG.format(path_or_url.split("?")[0]))
        if head == GZIP_MAGIC:
            return gzip.GzipFile(fileobj=buf), r.close
        return buf, r.close

    with open(path_or_url, "rb") as probe:
        head = probe.read(2)
    if head == ZIP_MAGIC:
        raise ValueError(_ZIP_MSG.format(path_or_url))
    f = gzip.open(path_or_url, "rb") if head == GZIP_MAGIC else open(path_or_url, "rb")
    return f, f.close


HEADER_FIELDS = ("reporting_entity_name", "reporting_entity_type",
                 "last_updated_on", "version")


def _network_names(item):
    """
    The network names on one provider_references item, as a set of strings.

    The schema says `network_name` is an array, and Aetna uses it that way --
    a single provider group can sit in eight networks at once. Cigna and UHC
    publish a one-element array. A bare string is accepted too, because
    payers are inconsistent about it and the cost of tolerating it is a line.
    """
    nets = item.get("network_name") or []
    if isinstance(nets, str):
        nets = [nets]
    # Joined with '|', not ',': network names are free text and may contain a
    # comma, while the NPIs and TINs in the sibling columns never can.
    return {str(n).replace("|", "/") for n in nets if n}


def read_header(path_or_url, probe=8192):
    """
    Return the file-level metadata that sits ahead of the rate data.

    {reporting_entity_name, reporting_entity_type, last_updated_on, version},
    with "" for anything absent.

    Matched with a regex over the first few KB rather than parsed as JSON:
    these four keys are always at the very top of the file, and a real parse
    would have to walk a multi-GB body to reach the closing brace. Both
    encodings in the wild are handled -- compact `"k":"v"` (Aetna, Cigna) and
    pretty-printed `"k": "v"` (UnitedHealthcare).

    Cheap enough to call on its own: it reads 8 KB, not the file.
    """
    src, closer = open_source(path_or_url)
    try:
        blob = src.read(probe).decode("utf-8", "replace")
    finally:
        if closer:
            closer()
    out = {}
    for key in HEADER_FIELDS:
        m = re.search(r'"%s"\s*:\s*"([^"]*)"' % re.escape(key), blob)
        out[key] = m.group(1) if m else ""
    return out


def build_relevant_groups(path_or_url, target_npis, target_tins=frozenset(),
                          progress=None, group_sizes=None, tin_names=None):
    """
    Pass 1. Return {provider_group_id(str): {'npis': csv, 'tins': csv,
    'matched_tins': csv}} for only the groups that contain at least one
    target NPI or are billed under a target TIN.

    Matching on TIN is what makes this work: the NPIs a payer lists for a
    hospital are rarely the ones an NPPES name search returns, but every
    provider group carries the tax ID it bills under, and a hospital has a
    handful of those.

    group_sizes, if given, is a dict this fills with
    {provider_group_id: number of distinct TINs} for EVERY group, relevant
    or not. Pass 2 uses it to say how many tax IDs share a rate -- a rate
    shared by 15,000 TINs is the payer's standard fee schedule, not a
    contract with one system.

    progress, if given, is called as progress(seen, kept) per item. Pass 1
    reads the whole provider_references block before pass 2 can start, which
    on a multi-GB file is many minutes, so callers should report something.

    tin_names, if given, is a dict this fills with {tin: business_name} for
    matched TINs only. The payer supplies a name on every tax ID; keeping
    them out of the rate rows and in a 120-row lookup instead is the
    difference between a dictionary-encoded column repeated 30M times and a
    table you can read at a glance.
    """
    src, closer = open_source(path_or_url)
    relevant = {}
    seen = 0
    try:
        events = _events_until_end_of(ijson.parse(src), "provider_references")
        for item in ijson_common.items(events, "provider_references.item"):
            seen += 1
            gid = item.get("provider_group_id")
            if gid is None:
                continue
            npis, tins = set(), set()
            names = {}
            for g in item.get("provider_groups", []):
                for npi in g.get("npi", []):       # schema 2.0: 'npi'
                    npis.add(str(npi))
                t = g.get("tin")
                if isinstance(t, dict) and t.get("value"):
                    tv = str(t["value"])
                    tins.add(tv)
                    if t.get("business_name"):
                        names[tv] = str(t["business_name"])
            if group_sizes is not None:
                group_sizes[str(gid)] = len(tins)
            hit = npis & target_npis
            tin_hit = tins & target_tins
            if hit or tin_hit:
                relevant[str(gid)] = {
                    "npis": ",".join(sorted(hit)),
                    "tins": ",".join(sorted(tins)),
                    "matched_tins": ",".join(sorted(tin_hit)),
                    "networks": "|".join(sorted(_network_names(item))),
                }
                if tin_names is not None:
                    for tv in tin_hit:
                        if tv in names:
                            tin_names.setdefault(tv, names[tv])
            if progress:
                progress(seen, len(relevant))
    finally:
        if closer:
            closer()
    return relevant


def stream_filtered_rates(path_or_url, relevant_groups, target_npis,
                          target_tins=frozenset(), max_records=None,
                          progress=None, group_sizes=None):
    """
    Pass 2. Yield flat rate dicts for rows touching any target provider,
    via references (variant A) or inline provider_groups (variant B).

    Each row carries `group_tins`: how many distinct tax IDs the rate is
    shared with, summed over the provider groups it references (needs
    group_sizes from pass 1; 0 when not available). The TINs themselves are
    not emitted: a network-wide group can hold 15,000 of them, and carrying
    that list on every row cost 14 GB of strings for one payer.

    Each row also carries `network_names`: the '|'-joined networks of the
    matched provider groups. Without it the payer label is the only clue to
    which network a rate belongs to, and that label is a hand-written config
    string -- one that names a single network even for an Aetna file whose
    groups span eight.
    """
    src, closer = open_source(path_or_url)
    sizes = group_sizes or {}
    n = 0
    try:
        events = _events_until_end_of(ijson.parse(src), "in_network")
        for item in ijson_common.items(events, "in_network.item"):
            bc = str(item.get("billing_code", ""))
            ct = item.get("billing_code_type", "")
            desc = item.get("description", "")
            for rg in item.get("negotiated_rates", []):
                m_npis, hit_tins, m_nets = set(), set(), set()
                width = 0

                # variant A: references into the provider_references block
                for ref in rg.get("provider_references", []):
                    rs = str(ref)
                    width += sizes.get(rs, 0)
                    g = relevant_groups.get(rs)
                    if g:
                        if g["npis"]:
                            m_npis.update(g["npis"].split(","))
                        if g.get("matched_tins"):
                            hit_tins.update(g["matched_tins"].split(","))
                        if g.get("networks"):
                            m_nets.update(g["networks"].split("|"))

                # variant B: inline provider_groups
                for g in rg.get("provider_groups", []):
                    t = g.get("tin")
                    tv = str(t["value"]) if isinstance(t, dict) and t.get("value") else ""
                    if tv:
                        width += 1
                    gn = {str(x) for x in g.get("npi", [])}
                    hit = gn & target_npis
                    if hit or tv in target_tins:
                        m_npis.update(hit)
                        if tv in target_tins:
                            hit_tins.add(tv)
                        # inline groups carry their own network_name, if any
                        m_nets.update(_network_names(g))

                if not m_npis and not hit_tins:
                    continue

                for pi in rg.get("negotiated_prices", []):
                    # CMS schema field is 'negotiated_rate'; some files/tools
                    # use 'negotiated_value'. Accept either.
                    val = pi.get("negotiated_rate", pi.get("negotiated_value"))
                    if val is None:
                        continue
                    sc = pi.get("service_code", [])
                    yield {
                        "billing_code": bc,
                        "code_type": ct,
                        "description": desc,
                        "negotiated_rate": float(val),
                        "rate_type": pi.get("negotiated_type", ""),
                        "billing_class": pi.get("billing_class", ""),
                        "service_codes": "|".join(sc) if isinstance(sc, list) else str(sc),
                        "expiration_date": pi.get("expiration_date", ""),
                        "matched_npis": ",".join(sorted(m_npis)),
                        "matched_tins": ",".join(sorted(hit_tins)),
                        "group_tins": width,
                        "network_names": "|".join(sorted(m_nets)),
                    }
                    n += 1
            if progress:
                progress(n)
            if max_records and n >= max_records:
                break
    finally:
        if closer:
            closer()
