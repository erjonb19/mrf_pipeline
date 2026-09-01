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

    # --- Cigna Health & Life Insurance Company -----------------------------
    # From the 2026-08-01 index, all group market. Signed URLs, but the
    # signatures run to 2036. OAP (Open Access Plus) is Cigna's broad
    # national network; Local Plus is deliberately narrower.
    # Re-run find_files.py on a fresh index if any of these ever 403.
    {"payer": "Cigna_PathwellOAP", "url": r"https://d25kgz5rikkq4n.cloudfront.net/cost_transparency/mrf/in-network-rates/reporting_month=2026-08/2026-08-01_cigna-health-life-insurance-company_pathwell-oap_in-network-rates.json.gz?Expires=2106187199&Signature=bxHcNWEGqIg0X-gvJlhMuqqSWIzGHlF9Dnai1edJtwU5eR9RZMxBli4n50g06DiNj9zAHPemolqjgKng-4sTMD7PexraEcd16VBJ7vougZkmBG18uFQ55t8QpU9YSqKjH-7AvtRG4256ZVzaK2i6rg-TDKSDn4Gtfm-cvkpJ0aLgmtj64HJ9dp3KZkP3Q9LNPZmnGUhgY001fEw0t5-SngN~7H5J5v~dlY7AJnxsX-YxFs4ICry6~FqkcctzUBy80tGdNpoFMaXOZxxMRLNQ8AXH9e8M-IouvWRI0B5b8ujOEft7plyBsx~Zf9WjQLUt6FaY3Jb9vgYHah9KW7upRA__&Key-Pair-Id=K1NVBEPVH9LWJP"},  # 1.59 GB, 20,856 plans, Open Access Plus - broadest
    {"payer": "Cigna_NationalOAP", "url": r"https://d25kgz5rikkq4n.cloudfront.net/cost_transparency/mrf/in-network-rates/reporting_month=2026-08/2026-08-01_cigna-health-life-insurance-company_national-oap_in-network-rates.json.gz?Expires=2106187199&Signature=db0S7I-wS5v~3fRRzP9H9Si2-SXJ4ydFBXkcXb92HR4AndiOEvWBwOjelT0Uia4umpXQP7bvuBx2R4JsinXcJTIpCotRKBSHbOJUlBW1mKwPdsVrUybwRIknFEGMIqKtD7dggoV4G37-sJZD3hkG8xsmAb4BzL~9avjZMeXXPV9ieCYDxrNUJhZyIoebbriGH4zybYSzacsOotBpqhtPO7F-TdHQT-EzOUl-G~bQ6~ZC646iJnv7AXAF6MJfO9H8AJ-URpUHyXuiDhtto-ZQTh82jAaru3Fo8HNNr9k~gLkv9dCECdOWsgXSRMoDHlH3KauTS6~ts10P7M7MWT-2OQ__&Key-Pair-Id=K1NVBEPVH9LWJP"},  # 1.59 GB,  3,808 plans, Open Access Plus
    {"payer": "Cigna_PathwellPPO", "url": r"https://d25kgz5rikkq4n.cloudfront.net/cost_transparency/mrf/in-network-rates/reporting_month=2026-08/2026-08-01_cigna-health-life-insurance-company_pathwell-ppo_in-network-rates.json.gz?Expires=2106187199&Signature=QyMTCcKSo7wO6FreVP65TqIQWdX5p5F-l5xm~QzofTUOT2TIdoo2oA9DteStxVZk-ywWF-E8spybTHBtbIalDVQ-ls~bjqiKlc~g2fzoz1Xe5aNj8E2BhXdqw83mj37Uy-SFM7JbLRfZ3~k7HuQZLErIDVf4laHvw6MousJ1Xjyd4WDL3ZUYYg6SV3f75GbKDg27RxPRROnElIUGVxmzgqa-TpbMMiprDi7uIdw8s-cy~aFYhl~VL31xa3F-iDD2B2oZxQtDb3w6j9PQYIqt~zvR0kUtsuPlAvaOV9II0USIwfHyUkxq-OHKZ5WIEIAA8p2VetTZcthK7sB-Xn1JCQ__&Key-Pair-Id=K1NVBEPVH9LWJP"},  # 1.58 GB,    367 plans, PPO
    {"payer": "Cigna_NationalPPO", "url": r"https://d25kgz5rikkq4n.cloudfront.net/cost_transparency/mrf/in-network-rates/reporting_month=2026-08/2026-08-01_cigna-health-life-insurance-company_national-ppo_in-network-rates.json.gz?Expires=2106187199&Signature=jZ~657Kk7wjiMWhClpfL8IziYAvhgIOtDWVqlnI28FSN4ix97G~BEWf~1KHjLRMkvqEPoxyRTm6-MfvGPHpGyoC9DO0TlvmRCJi570bQ0-iBILH1zQA4T~1s9uhTZ7LlFTYsDNbYmfP8t5X~EB4FKoTDG58g4k-2iceszrbdMS3vY8VIkbkJeLITDNsHVFu3uHiyXkrxWynwP0NsHzV2AbGL19mdaB-Rb7SALoQLgdno9JvvsaUnQ6ztB5UtMqXaG-AxiZiU5zBck2DMY4cUH8tsKOoXDQ4S2qQExLQrkG0sLn24XeiCOMr7wSonM3OiNpM1zDp3g4jgC7r6vu~7Uw__&Key-Pair-Id=K1NVBEPVH9LWJP"},  # 1.58 GB,    190 plans, PPO
    {"payer": "Cigna_LocalPlus", "url": r"https://d25kgz5rikkq4n.cloudfront.net/cost_transparency/mrf/in-network-rates/reporting_month=2026-08/2026-08-01_cigna-health-life-insurance-company_localplus-with-ebh-plus-pathwell_in-network-rates.json.gz?Expires=2106187199&Signature=HzAFxA3DysoSzxing1jwfisX2xiwTG90htS5XnwCJxY9RgyG04qNRezSvfMfjAhtPS-xEYXHG9zF1gQ~8KsCCIFqpKeVJWX7~lg8EroNLyUQ6HxfoXTOKOj3ZIBHUncptedncbAEQBQMdSR4IImdR7lT7gOotlTzpPdX0X0ikdefn7CerLCUyW77abYG01zFxS5lAL57Ch84qX9fVFsD9Eo7qJwuzCVAc6wWQyGUzyR1lVXrA~6G1u06MCirZ4q4AxFe13RsOwWejl4CDzp3XEwJuM2wIM32PkQGjJnxRiZL5NLcvAeqAogIV9DfHCsv7c4tTk33XkRQxr~dGuR4fw__&Key-Pair-Id=K1NVBEPVH9LWJP"},  # 1,659 plans, Local Plus (narrower)


    # --- Aetna Life Insurance Company (employer/EIN plans only) ------------
    # From the 2026-08-05 ALIC index. That index holds 382 plans, but 376 of
    # them carry HIOS marketplace ids and names beginning 'Aetna Exchange_'
    # while still declaring plan_market_type 'group' -- Aetna's market type
    # cannot be trusted. These 6 are the only ones with a real employer EIN
    # (066033492). Found with: find_files.py --id-type ein --medical
    {"payer": "AetnaALIC_OpenAccessElectChoice", "url": r"https://mrf.healthsparq.com/aetnacvs-egress.nophi.kyruushsq.com/prd/mrf/AETNACVS_I/ALICFI/2026-08-05/inNetworkRates/2026-08-05_pl-4yj-hr23_Aetna-Life-Insurance-Company.json.gz"},  # 3.76 GB, Aetna Open Access Elect Choice_60178
    {"payer": "AetnaALIC_OpenAccessManagedChoice", "url": r"https://mrf.healthsparq.com/aetnacvs-egress.nophi.kyruushsq.com/prd/mrf/AETNACVS_I/ALICFI/2026-08-05/inNetworkRates/2026-08-05_pl-53b-hr23-7cc371294e5f9eb289c1cc09f4ab43891dadd9e386371f6fd4448a9e938ccc2f_Aetna-Life-Insurance-Company.json.gz"},  # 6.79 GB, Aetna Open Access Managed Choice_60178
    {"payer": "AetnaALIC_OpenAccessHealthNetworkOption", "url": r"https://mrf.healthsparq.com/aetnacvs-egress.nophi.kyruushsq.com/prd/mrf/AETNACVS_I/ALICFI/2026-08-05/inNetworkRates/2026-08-05_pl-196-hr23_Aetna-Life-Insurance-Company.json.gz"},  # ?, Aetna Open Access HealthÂ NetworkÂ Option_60178
    {"payer": "AetnaALIC_Hmo", "url": r"https://mrf.healthsparq.com/aetnacvs-egress.nophi.kyruushsq.com/prd/mrf/AETNACVS_I/ALICFI/2026-08-05/inNetworkRates/2026-08-05_pl-2jf-hr23_Aetna-Life-Insurance-Company.json.gz"},  # 0.08 GB, Aetna HMO_60178
    {"payer": "AetnaALIC_Epo", "url": r"https://mrf.healthsparq.com/aetnacvs-egress.nophi.kyruushsq.com/prd/mrf/AETNACVS_I/ALICFI/2026-08-05/inNetworkRates/2026-08-05_pl-44p-hr23-8558cc664dc1ab706fd7b71b33e951a3a896391d91031a6bee0b8f61b04a2da0_Aetna-Life-Insurance-Company.json.gz"},  # 3.36 GB, Aetna EPO_60178
    {"payer": "AetnaALIC_Ppo", "url": r"https://mrf.healthsparq.com/aetnacvs-egress.nophi.kyruushsq.com/prd/mrf/AETNACVS_I/ALICFI/2026-08-05/inNetworkRates/2026-08-05_pl-4g6-hr23-1c054fd9cca277d346e9d15b5d675b41117c9760a54959900ffbf70ccfe40918_Aetna-Life-Insurance-Company.json.gz"},  # 4.37 GB, Aetna PPO_60170

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
