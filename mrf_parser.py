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
import requests

# Prefer the fast C backend; fall back to pure-Python ijson if unavailable.
try:
    import ijson.backends.yajl2_c as ijson
    BACKEND = "yajl2_c"
except Exception:  # pragma: no cover
    import ijson
    BACKEND = "python"


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
        if buf.peek(2)[:2] == GZIP_MAGIC:
            return gzip.GzipFile(fileobj=buf), r.close
        return buf, r.close

    with open(path_or_url, "rb") as probe:
        is_gz = probe.read(2) == GZIP_MAGIC
    f = gzip.open(path_or_url, "rb") if is_gz else open(path_or_url, "rb")
    return f, f.close


def build_relevant_groups(path_or_url, target_npis, progress=None):
    """
    Pass 1. Return {provider_group_id(str): {'npis': csv, 'tins': csv}} for
    only the groups that contain at least one target NPI.
    """
    src, closer = open_source(path_or_url)
    relevant = {}
    try:
        for item in ijson.items(src, "provider_references.item"):
            gid = item.get("provider_group_id")
            if gid is None:
                continue
            npis, tins = set(), set()
            for g in item.get("provider_groups", []):
                for npi in g.get("npi", []):       # schema 2.0: 'npi'
                    npis.add(str(npi))
                t = g.get("tin")
                if isinstance(t, dict) and t.get("value"):
                    tins.add(str(t["value"]))
            hit = npis & target_npis
            if hit:
                relevant[str(gid)] = {
                    "npis": ",".join(sorted(hit)),
                    "tins": ",".join(sorted(tins)),
                }
            if progress:
                progress(len(relevant))
    finally:
        if closer:
            closer()
    return relevant


def stream_filtered_rates(path_or_url, relevant_groups, target_npis,
                          max_records=None, progress=None):
    """
    Pass 2. Yield flat rate dicts for rows touching any target provider,
    via references (variant A) or inline provider_groups (variant B).
    """
    src, closer = open_source(path_or_url)
    n = 0
    try:
        for item in ijson.items(src, "in_network.item"):
            bc = str(item.get("billing_code", ""))
            ct = item.get("billing_code_type", "")
            desc = item.get("description", "")
            for rg in item.get("negotiated_rates", []):
                m_npis, m_tins = set(), set()

                # variant A: references into the provider_references block
                for ref in rg.get("provider_references", []):
                    rs = str(ref)
                    g = relevant_groups.get(rs)
                    if g:
                        m_npis.update(g["npis"].split(","))
                        if g["tins"]:
                            m_tins.update(g["tins"].split(","))

                # variant B: inline provider_groups
                for g in rg.get("provider_groups", []):
                    gn = {str(x) for x in g.get("npi", [])}
                    hit = gn & target_npis
                    if hit:
                        m_npis.update(hit)
                        t = g.get("tin")
                        if isinstance(t, dict) and t.get("value"):
                            m_tins.add(str(t["value"]))

                if not m_npis:
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
                        "tins": ",".join(sorted(m_tins)),
                    }
                    n += 1
            if progress:
                progress(n)
            if max_records and n >= max_records:
                break
    finally:
        if closer:
            closer()
