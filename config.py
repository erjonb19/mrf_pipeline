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
#
# Labels become the `payer` column and the parquet filename, so they must be
# unique. Use find_files.py --medical to pick these out of a payer's index
# rather than guessing from the portal's HTML listing.
UHC_NY = ("https://transparency-in-coverage.uhc.com/api/v1/uhc/blobs/download/"
          "2026-08-01/2026-08-01_UnitedHealthcare-Insurance-Company-of-New-York"
          "_Insurer_")

PAYER_FILES = [
    # --- Aetna -------------------------------------------------------------
    # Individual/exchange product. Only 125 of the 940 target NPIs appear in
    # it (1 of 73 for NYP, no Northwell hospitals), which is what a narrow
    # exchange network looks like. Kept for comparison; add an Aetna Life
    # Insurance Company (group) file alongside it when you have one.
    #
    # Label must stay "Aetna_NY": it matches the already-parsed
    # payer_parquet/Aetna_NY.parquet and the payer column inside it. Renaming
    # it orphans that file and triggers a full re-parse of the 2.9 GB source.
    {"payer": "Aetna_NY", "url": r"C:\Users\Erjon\Downloads\2026-06-05_pl-1qi-hr23_Aetna-Health-Insurance-Company-of-New-York.json\2026-06-05_pl-1qi-hr23_Aetna-Health-Insurance-Company-of-New-York.json"},

    # --- UnitedHealthcare, New York insurer entity -------------------------
    # All group (employer) market, from the 2026-08-01 index. Sizes are the
    # compressed download. Ordered smallest first so the cheap ones finish
    # before the expensive ones.
    {"payer": "UHC_NY_ChoicePlus",  "url": UHC_NY + "Choice-Plus_8_in-network-rates.json.gz"},        # 9.54 GB, 242 plans, broad POS
    {"payer": "UHC_NY_ChoiceEPO",   "url": UHC_NY + "Choice-EPO_561_in-network-rates.json.gz"},       # 9.54 GB, 291 plans, EPO
    {"payer": "UHC_NY_SelectEPO",   "url": UHC_NY + "Select-EPO_656_in-network-rates.json.gz"},       # 9.56 GB, 291 plans, EPO
    {"payer": "UHC_NY_POSChoicePlus", "url": UHC_NY + "PS1-50_C2_in-network-rates.json.gz"},          # 15.08 GB, 211 plans
    {"payer": "UHC_NY_ChoiceEPO50", "url": UHC_NY + "EP1-50_C1_in-network-rates.json.gz"},            # 15.08 GB, 89 plans
    {"payer": "UHC_NY_NationalPPO", "url": UHC_NY + "PP1-00_P3_in-network-rates.json.gz"},            # 15.42 GB, 28 plans
]
