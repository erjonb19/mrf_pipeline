"""
Pipeline configuration. Edit this file, not the others.
"""

# Path to the anchor list from step 1 (940 target NPIs).
TARGET_CSV = "target_providers.csv"

# Where downloaded raw MRF files are cached, and where parquet output lands.
CACHE_DIR = "mrf_cache"
OUTPUT_DIR = "payer_parquet"

# If True, download remote files once then parse locally. If False, stream.
DOWNLOAD_FIRST = True

# One entry per payer in-network file.
PAYER_FILES = [
    {"payer": "Aetna_NY", "url": r"C:\Users\Erjon\Downloads\2026-06-05_pl-1qi-hr23_Aetna-Health-Insurance-Company-of-New-York.json\2026-06-05_pl-1qi-hr23_Aetna-Health-Insurance-Company-of-New-York.json"},
]