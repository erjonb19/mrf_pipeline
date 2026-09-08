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
from urllib.parse import urlparse, unquote, parse_qs
import os
import zipfile
import re
import socket
import time
import urllib.error

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
#: Sent when a server refuses the browser agent above. Names the tool rather
#: than impersonating anything.
PLAIN_AGENT = "mrf-pipeline/0.1 (+price-transparency research)"

HTTP_HEADERS = {
    "Accept-Encoding": "identity",
    "Accept": "*/*",
    "User-Agent": ("Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                   "AppleWebKit/537.36 (KHTML, like Gecko) "
                   "Chrome/128.0.0.0 Safari/537.36"),
}


# Retry policy. Every source here is a multi-GB file from a payer CDN, and
# those fail transiently often enough that a single-shot GET throws away
# hours of work over a few seconds of network trouble. Bounded, because the
# other half of the failures are permanent: a signed URL that has expired
# returns 403 no matter how politely you ask again.
MAX_ATTEMPTS = 4
BACKOFF_BASE = 2.0     # seconds, squared each attempt: 2, 4, 8, ...
BACKOFF_CAP = 30.0

# Worth another attempt: overload, throttling, gateway trouble. 403 and 404
# are deliberately absent -- retrying those only turns a clear error into a
# slow one.
RETRY_STATUS = frozenset((408, 425, 429, 500, 502, 503, 504))


class TruncatedDownload(IOError):
    """
    A download ended before Content-Length said it would.

    Its own class so the retry policy can treat it as transient: a short read
    is exactly the failure that resuming fixes, and the alternative is a
    corrupt gzip that only reveals itself hours later, mid-parse.
    """


TRANSIENT_ERRORS = (
    requests.exceptions.ConnectionError,
    requests.exceptions.Timeout,
    requests.exceptions.ChunkedEncodingError,
    urllib.error.URLError,     # wraps DNS and socket failures
    ConnectionError,           # builtin: reset, aborted, broken pipe
    socket.timeout,
    TruncatedDownload,
)


def _status_of(exc):
    """The HTTP status an exception carries, or None if it is not one."""
    resp = getattr(exc, "response", None)          # requests.HTTPError
    if resp is not None and getattr(resp, "status_code", None):
        return resp.status_code
    return getattr(exc, "code", None)              # urllib.error.HTTPError


def is_transient(exc):
    """Whether another attempt could plausibly succeed."""
    status = _status_of(exc)
    if status is not None:
        return status in RETRY_STATUS
    return isinstance(exc, TRANSIENT_ERRORS)


def retry(fn, what, attempts=MAX_ATTEMPTS, log=print, sleep=None):
    """
    Call fn(), retrying transient network failures with exponential backoff.

    Re-raises immediately for anything permanent, and re-raises the last
    failure once the attempts are spent -- the caller still finds out, it
    just takes four tries first.

    `sleep` is resolved at call time rather than bound as a default, so tests
    can patch time.sleep and not actually wait out the backoff.
    """
    for attempt in range(1, attempts + 1):
        try:
            return fn()
        except Exception as e:
            if attempt == attempts or not is_transient(e):
                raise
            delay = min(BACKOFF_BASE ** attempt, BACKOFF_CAP)
            if log:
                log("    %s failed (%s: %s) -- attempt %d of %d, retrying "
                    "in %.0fs" % (what, type(e).__name__, e, attempt,
                                  attempts, delay))
            (sleep or time.sleep)(delay)


def _get_stream(url, log=print):
    """GET a URL, falling back to a plain agent when the browser one is refused.

    Emblemhealth's WAF answers the browser User-Agent with 406 -- the opposite
    of the usual problem, and the reason that header exists is that other payer
    CDNs refuse the default python-requests one. A plain agent naming this tool
    is accepted by both, and is the honest thing to send.
    """
    r = requests.get(url, stream=True, timeout=(10, 180), headers=HTTP_HEADERS)
    if r.status_code == 406:
        r.close()
        log("    406, retrying as a plain client: " + url.split("?")[0])
        r = requests.get(url, stream=True, timeout=(10, 180),
                         headers={"Accept": "*/*", "User-Agent": PLAIN_AGENT})
    r.raise_for_status()
    return r


def _zip_member(path, log=print):
    """Open the JSON payload inside a ZIP archive.

    A ZIP cannot be read from a stream: the central directory sits at the end
    of the file, so the reader has to seek. That is why a remote archive is
    downloaded before it is opened, and why this takes a local path only.

    Archives in this corpus hold one JSON file, sometimes beside a readme or a
    checksum. The largest member is taken rather than the first, because
    ordering inside an archive is not guaranteed and the rate data is always
    the biggest thing in it.
    """
    archive = zipfile.ZipFile(path)
    members = [m for m in archive.infolist() if not m.is_dir()]
    if not members:
        archive.close()
        raise ValueError(f"{path} is an empty ZIP archive")
    json_members = [m for m in members if m.filename.lower().endswith(".json")]
    chosen = max(json_members or members, key=lambda m: m.file_size)
    if len(members) > 1:
        log(f"    zip: {len(members)} members, reading {chosen.filename}")
    handle = archive.open(chosen)

    def close():
        handle.close()
        archive.close()

    # Buffered: ijson reads in small bites and ZipExtFile is slow unbuffered.
    return io.BufferedReader(handle, buffer_size=262144), close


def _download_zip(url, cache_dir, log=print):
    """Fetch a remote archive to disk so it can be seeked.

    The cache name comes from a `FileName` query parameter where the URL has
    one, and only otherwise from the path. Emblemhealth serves every archive
    from the same `/Home/GetFile` path, so naming by path alone would collide
    all of them onto one cache entry and silently serve the wrong file.

    Streamed to a `.part` and renamed on success, so an interrupted fetch is
    never mistaken for a complete archive on the next run.
    """
    os.makedirs(cache_dir, exist_ok=True)
    query = parse_qs(urlparse(url).query)
    name = (query.get("FileName") or query.get("filename") or [""])[0]
    if not name:
        name = unquote(os.path.basename(urlparse(url).path)) or "archive.zip"
    dest = os.path.join(cache_dir, os.path.basename(name))
    if os.path.exists(dest):
        log(f"    zip cached: {name}")
        return dest
    partial = dest + ".part"
    log(f"    downloading archive: {name}")
    with _get_stream(url, log) as r:
        with open(partial, "wb") as fh:
            for chunk in r.iter_content(chunk_size=1 << 20):
                if chunk:
                    fh.write(chunk)
    os.replace(partial, dest)
    return dest


def open_source(path_or_url, log=print, cache_dir="mrf_cache"):
    """
    Open a local or remote MRF uniformly, gzipped or not.

    Compression is detected from the first two bytes rather than from the
    file extension: payers serve plain .json from .gz URLs and vice versa,
    and signed URLs carry query strings that hide the real suffix.

    A remote open is retried on transient failure. Note that this covers
    establishing the stream, not reading it -- a connection that dies an hour
    into a parse cannot be resumed from here, which is why DOWNLOAD_FIRST
    exists.

    Returns (file_like, closer) where closer is a callable to release the
    underlying HTTP connection (or close the local file).
    """
    if path_or_url.startswith("http"):
        def connect():
            r = _get_stream(path_or_url, log)
            buf = io.BufferedReader(_ChunkedStream(r), buffer_size=262144)
            # Peek inside the retry, not after it. A CDN that accepts the
            # connection and then dies on the first chunk is a common
            # failure, and it only surfaces on the first read.
            try:
                return r, buf, buf.peek(2)[:2]
            except Exception:
                r.close()
                raise

        r, buf, head = retry(connect, "GET " + path_or_url.split("?")[0],
                             log=log)
        if head == ZIP_MAGIC:
            # A ZIP has to be seeked, so the stream is abandoned and the
            # archive fetched to disk instead. EmblemHealth publishes its
            # indexes this way.
            r.close()
            return _zip_member(_download_zip(path_or_url, cache_dir, log), log)
        if head == GZIP_MAGIC:
            return gzip.GzipFile(fileobj=buf), r.close
        return buf, r.close

    with open(path_or_url, "rb") as probe:
        head = probe.read(2)
    if head == ZIP_MAGIC:
        return _zip_member(path_or_url, log)
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


def read_header(path_or_url, probe=8192, log=print):
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
    src, closer = open_source(path_or_url, log=log)
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
