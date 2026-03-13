#!/usr/bin/env python3
"""
Card Spend Intelligence V4 — Production Notebook Builder
=========================================================
Generates: Card_Spend_Intelligence_V4_Master_Notebook.ipynb

One integrated pipeline across 3 phases:
  Phase 1: Taxonomy + Transaction Intelligence Foundation
  Phase 2: Customer-Month Feature Mart + Behavior Vectors
  Phase 3: Personas + Use-Case Layer

Run:  python build_card_spend_v4_production.py
"""
import json, os

def md(text):
    return {"cell_type": "markdown", "metadata": {}, "source": text.splitlines(True)}

def code(text):
    return {"cell_type": "code", "execution_count": None, "metadata": {}, "outputs": [], "source": text.splitlines(True)}

cells = []

# ═══════════════════════════════════════════════════════════════════════════════
# CELL 1: Title + Pipeline Overview (markdown)
# ═══════════════════════════════════════════════════════════════════════════════
cells.append(md("""# Card Spend Intelligence V4 — Production Master Notebook

**Single integrated pipeline — no competing versions.**

```
Transactions
→ Merchant Normalization + Token Extraction
→ Deterministic Merchant Intelligence (135+ Thailand rules)
→ Taxonomy Reconciliation (Rules > Mobius > B2K > Fallback)
→ Channel / Geography / Structural Tags
→ Behavioral Transaction Tags + Recurrence Detection
→ Customer-Month Base Aggregation
→ Spend Composition (L1 / L2 / Key L3)
→ Merchant Entropy / HHI / Concentration / Scale
→ Recurrence & Subscription Features
→ Wallet / Payment Behavior
→ Rolling Windows (3M / 6M)
→ Velocity / Trend / Migration
→ Spend Regime + Premium / Affluence + Risk Flags
→ Primary Persona Engine (10 personas, rolling 3M)
→ Micro-Segment Overlays (13 flags)
→ Campaign / CLM / Pricing / Risk Output Views
```

**Design Principles:**
1. **Point-in-time safe** — no future leakage into past months
2. **Deterministic taxonomy first** — rules + system taxonomies before fuzzy
3. **Monthly customer grain** for features and personas
4. **Transaction grain** for merchant intelligence and behavior tags
5. **Personas are slow-moving** (3M rolling), micro-segments are fast
6. **Wallet top-ups isolated** from true consumption
7. **Audit tables mandatory** at each phase boundary
"""))

# ═══════════════════════════════════════════════════════════════════════════════
# CELL 2: Phase 1 Pre-Build Audit (markdown)
# ═══════════════════════════════════════════════════════════════════════════════
cells.append(md("""## Phase 1 — Pre-Build Audit Decisions

### 1. Is DIGITAL_SERVICES an ecosystem or a channel?
**Decision:** Both, orthogonally. `DIGITAL_SERVICES` is an L1 spend ecosystem for actual digital
product/service spend (streaming, SaaS, gaming, app stores). `ONLINE/OFFLINE/IN_APP` are channel
tags capturing HOW a transaction was made. A grocery purchase on Shopee is
`SHOPPING_RETAIL.MARKETPLACE_ECOM` via `ONLINE` channel — not digital services.

### 2. Where does wallet top-up belong?
**Decision:** `FINANCIAL_SERVICES.DIGITAL_WALLET_TOPUP`. Top-ups are funding events, not consumption.
They inflate lifestyle spend if mixed in. Isolated with `is_topup`, `is_wallet`,
`is_processor_mediated` flags. Features separate wallet/topup spend from consumption metrics.

### 3. Should deterministic rules outrank Mobius/B2K?
**Decision:** Yes, for high-confidence merchant-specific rules. Curated analyst-verified patterns
for known Thai merchants have higher precision than generic MCC-based classification.
B2K/Mobius handle the long tail where no specific rule exists. Source conflicts flagged for audit.

### 4. Mall and department store ambiguity
**Decision:** `Central` defaults to `SHOPPING_RETAIL.MALLS_DEPARTMENT.CENTRAL_GROUP` as the entity.
Tenant-level classification requires transaction-level merchant granularity beyond what most descriptors
provide. The Mall Group, Siam district, Mega malls each get their own L3 archetype.

### 5. Avoiding OTHER_LONGTAIL dumping
**Decision:** Multi-level fallback: deterministic rules → Mobius → B2K → UNCLASSIFIED.
Never force into a specific L1 without evidence. Track unclassified rate. Target <15%.
If higher, expand merchant rules.

### Key risks
- Source columns: `CUST_NUM`, `MCHT_NM`, `TXN_AMT`, `TXN_DT` are **required**. Others optional.
- B2K/Mobius reference tables are **placeholders** — must be replaced with real data.
- Regex rules are deterministic seeds — precision > recall philosophy.
- Time-of-day features only if `TXN_DT` has real timestamp precision.
"""))

# ═══════════════════════════════════════════════════════════════════════════════
# CELL 3: Imports + Global Config
# ═══════════════════════════════════════════════════════════════════════════════
cells.append(code("""# COMMAND ----------
# ══════════════════════════════════════════════════════════════
# IMPORTS & GLOBAL CONFIG
# ══════════════════════════════════════════════════════════════
from pyspark.sql import functions as F, Window
from pyspark.sql.types import *
from datetime import datetime
import re

# ── TABLE CONFIG ──
SRC_TABLE           = "cdx_mdz_prd.cdx_curated_crcard_acl_db.crcard_txn_dly"
PHASE1_OUTPUT_TABLE = "tmp.card_spend_txn_taxonomy_v4"
PHASE2_OUTPUT_TABLE = "tmp.card_spend_feature_mart_v4"
PHASE3_OUTPUT_TABLE = "tmp.card_spend_persona_v4"
AUDIT_TABLE         = "tmp.card_spend_pipeline_audit_v4"

# ── PATTERN CONFIG ──
TOPUP_PATTERNS = [
    "truemoney", "true money", "rabbit line pay", "linepay", "line pay",
    "shopeepay", "blueplus", "wallet topup", "wallet top-up", "topup", "top-up",
    "\u0e40\u0e15\u0e34\u0e21\u0e40\u0e07\u0e34\u0e19", "reload", "cash in",
    "purchase/load", "funding transaction", "poi funding", "stored value"
]

WALLET_PATTERNS = [
    "truemoney", "true money", "rabbit line pay", "linepay", "line pay",
    "shopeepay", "blueplus", "promptpay", "grabpay", "grab pay",
    "airpay", "dolfin", "kplus", "k\\\\+", "scb easy",
    "samsung pay", "apple pay", "google pay"
]

PROCESSOR_PATTERNS = [
    "paypal", "stripe", "adyen", "worldpay", "2c2p", "omise", "opn payments"
]

IN_APP_PATTERNS = [
    "grab", "lineman", "line man", "foodpanda", "shopee", "lazada",
    "truemoney", "linepay", "rabbit line pay", "netflix", "spotify",
    "robinhood"
]

FOOD_DELIVERY_PATTERNS = [
    "grabfood", "grab food", "foodpanda", "lineman", "line man", "robinhood"
]

RIDE_HAILING_PATTERNS = [
    "grab(?!food|\\\\s?food)", "bolt", "grabcar", "grabtaxi", "indriver"
]

SUBSCRIPTION_PATTERNS = [
    "netflix", "spotify", "youtube premium", "disney", "hbo",
    "viu", "wetv", "iqiyi", "true id", "ais play",
    "apple music", "joox", "adobe", "canva", "figma",
    "microsoft 365", "ms 365", "icloud"
]

RISKY_PATTERNS = [
    "casino", "gambling", "\\\\bbet\\\\b", "\\\\bslot\\\\b", "lottery",
    "crypto", "forex", "binary option", "pawn shop",
    "escort", "adult"
]

GROCERY_PATTERNS = [
    "big c", "bigc", "lotus", "tesco", "tops market", "\\\\btops\\\\b",
    "makro", "gourmet market", "villa market", "foodland",
    "cp fresh", "maxvalu"
]

MALL_PATTERNS = [
    "central world", "central plaza", "central festival",
    "the mall", "terminal 21", "fashion island", "mega bangna",
    "future park", "seacon", "jungceylon", "westgate",
    "iconsiam", "paragon", "emporium", "emquartier"
]

PREMIUM_BRAND_PATTERNS = [
    "louis vuitton", "gucci", "prada", "chanel", "hermes", "dior",
    "burberry", "bottega", "balenciaga", "ysl", "saint laurent",
    "cartier", "tiffany", "bvlgari", "omega", "rolex"
]

# ── ROLLING WINDOW CONFIG ──
ROLLING_3M  = 2    # current + 2 prior rows
ROLLING_6M  = 5
ROLLING_12M = 11

# ── HELPER: word boundary wrapper for regex ──
def wb(text):
    \"\"\"Wrap a keyword with regex word boundaries for precise matching.\"\"\"
    return "\\\\b" + text + "\\\\b"
"""))

# ═══════════════════════════════════════════════════════════════════════════════
# CELL 4: Source Read + Defensive Schema
# ═══════════════════════════════════════════════════════════════════════════════
cells.append(code("""# COMMAND ----------
# ══════════════════════════════════════════════════════════════
# SOURCE READ + DEFENSIVE SCHEMA
# ══════════════════════════════════════════════════════════════
txn_raw = spark.table(SRC_TABLE)

# Required columns
REQUIRED_COLS = ["CUST_NUM", "MCHT_NM", "TXN_AMT", "TXN_DT"]
for c in REQUIRED_COLS:
    assert c in txn_raw.columns, f"REQUIRED column missing: {c}"

# Optional columns — create NULL placeholders if missing
OPTIONAL_COLS = {
    "TXN_DESC": "string",
    "CURR_DESC": "string",
    "ONLN_FLAG": "string",
    "MCHT_CATG": "string",       # B2K category
    "MCHT_SUB_CATG": "string",   # B2K sub-category
    "SIC_CD_DESC": "string",     # Mobius SIC
    "MCC_CD": "string",          # MCC code
    "CNTRY_CD": "string",        # country code
    "TXN_CURR_CD": "string",     # transaction currency code
}

for col_name, col_type in OPTIONAL_COLS.items():
    if col_name not in txn_raw.columns:
        txn_raw = txn_raw.withColumn(col_name, F.lit(None).cast(col_type))

# Add synthetic row identifier for deterministic joins
txn = (
    txn_raw
    .withColumn("_txn_row_id", F.monotonically_increasing_id())
    .withColumn("txn_date", F.to_date("TXN_DT"))
    .withColumn("txn_ts", F.to_timestamp("TXN_DT"))
    .withColumn("txn_month", F.date_trunc("month", F.to_timestamp("TXN_DT")).cast("date"))
    .withColumn("txn_amount", F.col("TXN_AMT").cast("double"))
)

print(f"Source row count: {txn.count()}")
print(f"Columns: {txn.columns}")
"""))

# ═══════════════════════════════════════════════════════════════════════════════
# CELL 5: Normalization + Token Extraction
# ═══════════════════════════════════════════════════════════════════════════════
cells.append(code("""# COMMAND ----------
# ══════════════════════════════════════════════════════════════
# MERCHANT NORMALIZATION + TOKEN EXTRACTION
# ══════════════════════════════════════════════════════════════

def normalize_merchant(col):
    \"\"\"Multi-step merchant descriptor normalization.
    Steps: lowercase → trim → collapse whitespace → remove noise symbols
    → strip branch/outlet suffixes → strip city/country tails → remove long numbers.
    \"\"\"
    c = F.lower(F.trim(F.coalesce(col, F.lit(""))))
    c = F.regexp_replace(c, r"[*#@!{}]+", " ")             # noise symbols
    c = F.regexp_replace(c, r"\\s+", " ")                   # collapse whitespace
    c = F.regexp_replace(c, r"\\b(branch|br|stn|station|outlet|kiosk)\\s*\\d*\\b", "")  # branch suffixes
    c = F.regexp_replace(c, r"\\b(bangkok|bkk|chiang\\s?mai|phuket|pattaya|hat\\s?yai|korat|khon\\s?kaen|udon|thailand|th)\\s*$", "")  # city/country tails
    c = F.regexp_replace(c, r"\\b\\d{5,}\\b", "")            # long numeric codes (merchant IDs etc)
    c = F.regexp_replace(c, r"\\s+", " ")                   # re-collapse
    c = F.trim(c)
    return c

txn1 = (
    txn
    .withColumn("mcht_nm_raw",  F.col("MCHT_NM"))
    .withColumn("mcht_nm_norm", normalize_merchant(F.col("MCHT_NM")))
    .withColumn("txn_desc_norm", normalize_merchant(F.coalesce(F.col("TXN_DESC"), F.lit(""))))
    .withColumn("match_text",   F.concat_ws(" | ", F.col("mcht_nm_norm"), F.col("txn_desc_norm")))
    .withColumn("merchant_key", F.sha2(F.col("mcht_nm_norm"), 256))
    # Token extraction
    .withColumn("merchant_tokens",    F.split(F.col("mcht_nm_norm"), r"\\s+"))
    .withColumn("merchant_token_cnt", F.size("merchant_tokens"))
    .withColumn("merchant_brand_root",
        F.when(F.size("merchant_tokens") >= 1, F.element_at("merchant_tokens", 1))
    )
)

display(txn1.select("mcht_nm_raw", "mcht_nm_norm", "match_text", "merchant_brand_root").limit(30))
"""))

# ═══════════════════════════════════════════════════════════════════════════════
# CELL 6: Taxonomy Hierarchy Reference
# ═══════════════════════════════════════════════════════════════════════════════
cells.append(code("""# COMMAND ----------
# ══════════════════════════════════════════════════════════════
# L1 / L2 / L3 TAXONOMY HIERARCHY (CANONICAL REFERENCE)
# ══════════════════════════════════════════════════════════════
# This is the authoritative taxonomy. All rules and reconciliation map INTO this.

TAXONOMY_HIERARCHY = {
    "EVERYDAY_ESSENTIALS": {
        "GROCERY_MODERN":     ["RETAIL_GROCERY_MODERN", "PREMIUM_GROCERY"],
        "CONVENIENCE_STORE":  ["CONVENIENCE_CHAIN"],
        "WHOLESALE":          ["WHOLESALE_CLUB"],
        "PHARMACY_DRUGSTORE": ["PHARMACY_CHAIN"],
        "UTILITIES":          ["WATER_ELECTRIC_GAS"],
        "FUEL":               ["FUEL_STATION"],
    },
    "FOOD_DINING": {
        "RESTAURANTS":    ["THAI_CHAIN_RESTAURANTS", "INTERNATIONAL_CHAIN", "FINE_DINING"],
        "FAST_FOOD_QSR":  ["FAST_FOOD_CHAIN"],
        "COFFEE_CAFE":    ["COFFEE_CHAINS", "INDEPENDENT_CAFE"],
        "FOOD_DELIVERY":  ["FOOD_DELIVERY_APP"],
        "BAKERY_DESSERT": ["BAKERY_CHAIN"],
    },
    "TRAVEL_MOBILITY": {
        "RIDE_HAILING":    ["RIDE_HAILING_APP"],
        "PUBLIC_TRANSPORT": ["BTS_MRT_BUS"],
        "TOLL_PARKING":    ["EXPRESSWAY_TOLL", "PARKING"],
        "AIRLINES":        ["DOMESTIC_AIRLINE", "INTERNATIONAL_AIRLINE"],
        "HOTELS":          ["HOTEL_CHAIN", "BOUTIQUE_HOTEL"],
        "OTA_TRAVEL":      ["OTA_PLATFORM", "ACTIVITY_BOOKING"],
    },
    "SHOPPING_RETAIL": {
        "MALLS_DEPARTMENT":  ["CENTRAL_GROUP_MALLS", "THE_MALL_GROUP", "SIAM_EM_DISTRICT",
                              "MEGA_MALLS", "DEPARTMENT_STORES", "ICONSIAM"],
        "FASHION":           ["COMMON_FASHION_BRANDS", "LUXURY_FASHION_BRANDS", "SPORTSWEAR"],
        "ELECTRONICS":       ["ELECTRONICS_RETAIL"],
        "BOOKS_STATIONERY":  ["BOOKSTORE_CHAIN"],
        "GIFTS_HOBBY_TOYS":  ["SPECIALITY_GIFTSHOP", "TOY_HOBBY"],
        "MARKETPLACE_ECOM":  ["MARKETPLACE_PLATFORM"],
        "VARIETY_HOME":      ["VARIETY_STORE"],
    },
    "HEALTH_WELLNESS": {
        "HOSPITAL_CLINIC":   ["HOSPITAL_PRIVATE", "CLINIC"],
        "BEAUTY_WELLNESS":   ["BEAUTY_RETAIL", "SPA_WELLNESS"],
        "FITNESS_GYM":       ["GYM_FITNESS"],
    },
    "DIGITAL_SERVICES": {
        "STREAMING":     ["STREAMING_CONTENT", "MUSIC_STREAMING"],
        "GAMING":        ["GAMING_PLATFORM"],
        "SAAS_BIG_TECH": ["GOOGLE_ECOSYSTEM", "META_ECOSYSTEM", "MICROSOFT_ECOSYSTEM", "APPLE_ECOSYSTEM"],
        "TELCO_MOBILE":  ["TELCO_MOBILE_PROVIDER"],
        "TELCO_BROADBAND": ["TELCO_BROADBAND_PROVIDER"],
    },
    "ENTERTAINMENT_LEISURE": {
        "CINEMA":           ["CINEMA_CHAIN"],
        "ATTRACTIONS":      ["THEME_PARK", "MUSEUM_ZOO"],
        "SPORTS_ACTIVITY":  ["SPECIALITY_SPORT", "GOLF"],
    },
    "EDUCATION": {
        "SCHOOL_UNIVERSITY": ["UNIVERSITY", "SCHOOL"],
        "TUTORING_LANGUAGE": ["TUTORING_CENTER"],
    },
    "HOME_LIVING": {
        "HOME_IMPROVEMENT": ["HOME_IMPROVEMENT_STORE"],
        "FURNITURE_HOME":   ["FURNITURE_STORE"],
    },
    "AUTOMOTIVE": {
        "VEHICLE_SERVICE": ["CAR_SERVICE"],
        "CAR_RENTAL":      ["CAR_RENTAL_AGENCY"],
    },
    "FINANCIAL_SERVICES": {
        "DIGITAL_WALLET_TOPUP":  ["TRUEMONEY", "LINEPAY", "SHOPEEPAY"],
        "PAYMENT_PROCESSORS":    ["PAYMENT_PROCESSOR"],
        "INSURANCE":             ["INSURANCE_PAYMENTS"],
        "INVESTMENT_TRADING":    ["TRADING_PLATFORM"],
    },
    "B2B_PROFESSIONAL": {
        "GOVERNMENT":            ["GOVERNMENT_SERVICE"],
        "PROFESSIONAL_SERVICES": ["OFFICE_SUPPLY", "COURIER_LOGISTICS"],
    },
    "OTHER_LONGTAIL": {
        "UNCLASSIFIED": ["UNCLASSIFIED"],
    },
}

# Flatten into reference DataFrame for validation
taxonomy_rows = []
for l1, l2_dict in TAXONOMY_HIERARCHY.items():
    for l2, l3_list in l2_dict.items():
        for l3 in l3_list:
            taxonomy_rows.append((l1, l2, l3))

taxonomy_ref = spark.createDataFrame(taxonomy_rows, ["ref_l1", "ref_l2", "ref_l3"])
print(f"Taxonomy reference: {taxonomy_ref.count()} L3 archetypes across {len(TAXONOMY_HIERARCHY)} L1 ecosystems")
display(taxonomy_ref)
"""))

# ═══════════════════════════════════════════════════════════════════════════════
# CELL 7: Deterministic Merchant Rules (~120 rules)
# ═══════════════════════════════════════════════════════════════════════════════
cells.append(code("""# COMMAND ----------
# ══════════════════════════════════════════════════════════════
# DETERMINISTIC MERCHANT RULES — THAILAND PRODUCTION SEED
# ══════════════════════════════════════════════════════════════
# Priority: lower number = higher priority = checked first.
# Specific rules (GrabFood) must have lower priority number than generic (Grab).
# Format: (priority, rule_name, regex_pattern, L1, L2, L3)
#
# NOTE: In production, store this as a governed Delta table.
#       This seed covers ~120 rules across all L2 categories.

merchant_rules = [
    # ── WALLETS / TOPUPS / PROCESSORS (1-9) ──────────────────
    (1,  "TRUEMONEY",     "truemoney|true money|true\\\\s?wallet",     "FINANCIAL_SERVICES", "DIGITAL_WALLET_TOPUP", "TRUEMONEY"),
    (2,  "LINEPAY",       "rabbit line pay|linepay|line pay",         "FINANCIAL_SERVICES", "DIGITAL_WALLET_TOPUP", "LINEPAY"),
    (3,  "SHOPEEPAY",     "shopeepay|shopee\\\\s?pay",                  "FINANCIAL_SERVICES", "DIGITAL_WALLET_TOPUP", "SHOPEEPAY"),
    (4,  "BLUEPAY",       "blueplus|blue\\\\s?pay",                     "FINANCIAL_SERVICES", "DIGITAL_WALLET_TOPUP", "TRUEMONEY"),
    (5,  "PAYPAL",        "paypal",                                   "FINANCIAL_SERVICES", "PAYMENT_PROCESSORS",   "PAYMENT_PROCESSOR"),
    (6,  "STRIPE",        "stripe|adyen|worldpay|2c2p|omise",        "FINANCIAL_SERVICES", "PAYMENT_PROCESSORS",   "PAYMENT_PROCESSOR"),
    (7,  "PROMPTPAY",     "promptpay|prompt\\\\s?pay",                  "FINANCIAL_SERVICES", "DIGITAL_WALLET_TOPUP", "TRUEMONEY"),

    # ── FOOD DELIVERY (10-15) ────────────────────────────────
    (10, "GRABFOOD",      "grabfood|grab\\\\s?food",                    "FOOD_DINING", "FOOD_DELIVERY",  "FOOD_DELIVERY_APP"),
    (11, "FOODPANDA",     "foodpanda|food\\\\s?panda",                  "FOOD_DINING", "FOOD_DELIVERY",  "FOOD_DELIVERY_APP"),
    (12, "LINEMAN",       "lineman|line\\\\s?man",                      "FOOD_DINING", "FOOD_DELIVERY",  "FOOD_DELIVERY_APP"),
    (13, "ROBINHOOD_FD",  "robinhood",                                "FOOD_DINING", "FOOD_DELIVERY",  "FOOD_DELIVERY_APP"),

    # ── RIDE-HAILING / TRANSPORT (20-29) ─────────────────────
    (20, "GRAB_RIDE",     "grab(?!food|\\\\s?food)",                    "TRAVEL_MOBILITY", "RIDE_HAILING",     "RIDE_HAILING_APP"),
    (21, "BOLT",          "bolt\\\\s?(ride|taxi)?",                     "TRAVEL_MOBILITY", "RIDE_HAILING",     "RIDE_HAILING_APP"),
    (22, "BTS",           "bts|skytrain|sky\\\\s?train",                "TRAVEL_MOBILITY", "PUBLIC_TRANSPORT",  "BTS_MRT_BUS"),
    (23, "MRT",           "\\\\bmrt\\\\b",                                "TRAVEL_MOBILITY", "PUBLIC_TRANSPORT",  "BTS_MRT_BUS"),
    (24, "EXPRESSWAY",    "expressway|tollway|toll\\\\s?way|exat",      "TRAVEL_MOBILITY", "TOLL_PARKING",     "EXPRESSWAY_TOLL"),
    (25, "EASY_PASS",     "easy\\\\s?pass",                             "TRAVEL_MOBILITY", "TOLL_PARKING",     "EXPRESSWAY_TOLL"),

    # ── MARKETPLACES / E-COMMERCE (30-39) ────────────────────
    (30, "SHOPEE",        "shopee(?!pay|\\\\s?pay)",                    "SHOPPING_RETAIL", "MARKETPLACE_ECOM", "MARKETPLACE_PLATFORM"),
    (31, "LAZADA",        "lazada",                                   "SHOPPING_RETAIL", "MARKETPLACE_ECOM", "MARKETPLACE_PLATFORM"),
    (32, "AMAZON",        "amazon|amzn",                              "SHOPPING_RETAIL", "MARKETPLACE_ECOM", "MARKETPLACE_PLATFORM"),
    (33, "ALIEXPRESS",    "aliexpress|ali\\\\s?express|alibaba",        "SHOPPING_RETAIL", "MARKETPLACE_ECOM", "MARKETPLACE_PLATFORM"),
    (34, "JD_CENTRAL",    "jd\\\\s?central|jd\\\\.co",                   "SHOPPING_RETAIL", "MARKETPLACE_ECOM", "MARKETPLACE_PLATFORM"),
    (35, "TIKTOK_SHOP",   "tiktok\\\\s?shop",                          "SHOPPING_RETAIL", "MARKETPLACE_ECOM", "MARKETPLACE_PLATFORM"),

    # ── STREAMING / DIGITAL SERVICES (40-49) ─────────────────
    (40, "NETFLIX",       "netflix",                                  "DIGITAL_SERVICES", "STREAMING",     "STREAMING_CONTENT"),
    (41, "SPOTIFY",       "spotify",                                  "DIGITAL_SERVICES", "STREAMING",     "MUSIC_STREAMING"),
    (42, "YOUTUBE",       "youtube|google\\\\*youtube",                 "DIGITAL_SERVICES", "STREAMING",     "STREAMING_CONTENT"),
    (43, "DISNEY_PLUS",   "disney\\\\+|disney\\\\s?plus|hotstar",       "DIGITAL_SERVICES", "STREAMING",     "STREAMING_CONTENT"),
    (44, "HBO",           "hbo|hbo\\\\s?go",                            "DIGITAL_SERVICES", "STREAMING",     "STREAMING_CONTENT"),
    (45, "APPLE_DIGITAL", "apple\\\\.com|itunes|app\\\\s?store",        "DIGITAL_SERVICES", "SAAS_BIG_TECH", "APPLE_ECOSYSTEM"),
    (46, "GOOGLE_PLAY",   "google\\\\s?(play|one|cloud|storage)",      "DIGITAL_SERVICES", "SAAS_BIG_TECH", "GOOGLE_ECOSYSTEM"),
    (47, "GOOGLE_ADS",    "google\\\\s?(ads|adwords)",                 "B2B_PROFESSIONAL", "PROFESSIONAL_SERVICES", "OFFICE_SUPPLY"),
    (48, "MICROSOFT",     "microsoft|xbox|office\\\\s?365|azure",      "DIGITAL_SERVICES", "SAAS_BIG_TECH", "MICROSOFT_ECOSYSTEM"),
    (49, "META",          "meta\\\\s?platforms|facebook\\\\s?(ads|pay)", "DIGITAL_SERVICES", "SAAS_BIG_TECH", "META_ECOSYSTEM"),
    (50, "STEAM",         "steam|steampowered|valve",                 "DIGITAL_SERVICES", "GAMING",        "GAMING_PLATFORM"),
    (51, "LINE_STICKER",  "line\\\\s?(sticker|store|game|webtoon)",    "DIGITAL_SERVICES", "GAMING",        "GAMING_PLATFORM"),

    # ── GROCERY / MODERN TRADE (55-64) ───────────────────────
    (55, "BIGC",          "big\\\\s?c(?!inema)",                        "EVERYDAY_ESSENTIALS", "GROCERY_MODERN",    "RETAIL_GROCERY_MODERN"),
    (56, "LOTUS",         "lotus|tesco\\\\s?lotus",                     "EVERYDAY_ESSENTIALS", "GROCERY_MODERN",    "RETAIL_GROCERY_MODERN"),
    (57, "TOPS",          "tops\\\\s?(market|daily|super)?",            "EVERYDAY_ESSENTIALS", "GROCERY_MODERN",    "RETAIL_GROCERY_MODERN"),
    (58, "GOURMET_MKT",   "gourmet\\\\s?market|villa\\\\s?market",       "EVERYDAY_ESSENTIALS", "GROCERY_MODERN",    "PREMIUM_GROCERY"),
    (59, "FOODLAND",      "foodland",                                 "EVERYDAY_ESSENTIALS", "GROCERY_MODERN",    "RETAIL_GROCERY_MODERN"),
    (60, "MAXVALU",       "maxvalu|max\\\\s?value",                     "EVERYDAY_ESSENTIALS", "GROCERY_MODERN",    "RETAIL_GROCERY_MODERN"),

    # ── CONVENIENCE STORES (65-69) ───────────────────────────
    (65, "SEVEN11",       "7[\\\\s\\\\-/]?eleven|7/11|seven.eleven",     "EVERYDAY_ESSENTIALS", "CONVENIENCE_STORE", "CONVENIENCE_CHAIN"),
    (66, "FAMILYMART",    "familymart|family\\\\s?mart",                "EVERYDAY_ESSENTIALS", "CONVENIENCE_STORE", "CONVENIENCE_CHAIN"),
    (67, "LAWSON",        "lawson|108\\\\s?(shop|mart)?",               "EVERYDAY_ESSENTIALS", "CONVENIENCE_STORE", "CONVENIENCE_CHAIN"),
    (68, "CJ_EXPRESS",    "cj\\\\s?(express|more|supermarket)",         "EVERYDAY_ESSENTIALS", "CONVENIENCE_STORE", "CONVENIENCE_CHAIN"),
    (69, "MINI_BIGC",     "mini\\\\s?big\\\\s?c",                        "EVERYDAY_ESSENTIALS", "CONVENIENCE_STORE", "CONVENIENCE_CHAIN"),

    # ── WHOLESALE (70-72) ────────────────────────────────────
    (70, "MAKRO",         "makro|siam\\\\s?makro",                      "EVERYDAY_ESSENTIALS", "WHOLESALE",     "WHOLESALE_CLUB"),

    # ── FUEL (75-79) ─────────────────────────────────────────
    (75, "PTT",           "\\\\bptt\\\\b|ptt\\\\s?(station|oil|public)",  "EVERYDAY_ESSENTIALS", "FUEL", "FUEL_STATION"),
    (76, "SHELL",         "\\\\bshell\\\\b",                              "EVERYDAY_ESSENTIALS", "FUEL", "FUEL_STATION"),
    (77, "BANGCHAK",      "bangchak",                                 "EVERYDAY_ESSENTIALS", "FUEL", "FUEL_STATION"),
    (78, "CALTEX",        "caltex|chevron",                           "EVERYDAY_ESSENTIALS", "FUEL", "FUEL_STATION"),
    (79, "ESSO",          "\\\\besso\\\\b|exxon",                        "EVERYDAY_ESSENTIALS", "FUEL", "FUEL_STATION"),
    (80, "SUSCO",         "susco",                                    "EVERYDAY_ESSENTIALS", "FUEL", "FUEL_STATION"),

    # ── PHARMACY / HEALTH / PERSONAL CARE (82-99b) ──────────
    (82, "BOOTS",         "\\\\bboots\\\\b",                              "EVERYDAY_ESSENTIALS", "PHARMACY_DRUGSTORE", "PHARMACY_CHAIN"),
    (83, "WATSONS",       "watsons|watson",                           "HEALTH_WELLNESS", "BEAUTY_WELLNESS",   "BEAUTY_RETAIL"),
    (84, "FASCINO",       "fascino",                                  "HEALTH_WELLNESS", "BEAUTY_WELLNESS",   "BEAUTY_RETAIL"),
    (85, "BUMRUNGRAD",    "bumrungrad",                               "HEALTH_WELLNESS", "HOSPITAL_CLINIC",   "HOSPITAL_PRIVATE"),
    (86, "BKKHOSP",       "bangkok\\\\s?hospital|bdms|bangkok\\\\s?dusit","HEALTH_WELLNESS", "HOSPITAL_CLINIC",   "HOSPITAL_PRIVATE"),
    (87, "SAMITIVEJ",     "samitivej",                                "HEALTH_WELLNESS", "HOSPITAL_CLINIC",   "HOSPITAL_PRIVATE"),
    (88, "BNH",           "\\\\bbnh\\\\b",                                "HEALTH_WELLNESS", "HOSPITAL_CLINIC",   "HOSPITAL_PRIVATE"),
    (89, "BEAUTY_SPA",    "spa\\\\b|massage|onsen|let.s relax",        "HEALTH_WELLNESS", "BEAUTY_WELLNESS",   "SPA_WELLNESS"),

    # ── INCREMENTAL: Healthcare gaps ─────────────────────────
    # Generic hospital/clinic/lab patterns (lower priority than named hospitals above)
    (240,"HOSPITAL_GEN",  "hospital|\\\\bhosp\\\\b|medical\\\\s?center",  "HEALTH_WELLNESS", "HOSPITAL_CLINIC",   "HOSPITAL_GENERAL"),
    (241,"CLINIC_GEN",    "clinic(?!.*beauty|.*cosmetic|.*skin|.*derma)","HEALTH_WELLNESS", "HOSPITAL_CLINIC",   "CLINIC_GENERAL"),
    (242,"DIAGNOSTIC_LAB","\\\\blab\\\\b|laboratory|diagnostic|pathology|radiology|x-?ray|mri\\\\b|ct\\\\s?scan",
                                                                      "HEALTH_WELLNESS", "DIAGNOSTICS",       "DIAGNOSTIC_LAB"),
    (243,"DENTAL",        "dental|dentist|orthodont|\\\\btooth\\\\b",    "HEALTH_WELLNESS", "HOSPITAL_CLINIC",   "DENTAL_CLINIC"),
    (244,"OPTICAL",       "optical|optician|eye\\\\s?care|\\\\blens\\\\b|spec\\\\s?saver|top\\\\s?charoen",
                                                                      "HEALTH_WELLNESS", "HOSPITAL_CLINIC",   "OPTICAL_CLINIC"),

    # ── INCREMENTAL: Personal care gaps ──────────────────────
    (245,"SALON",         "salon|hair\\\\s?(cut|dresser|studio)|barber|\\\\bcuts\\\\b",
                                                                      "HEALTH_WELLNESS", "BEAUTY_WELLNESS",   "BEAUTY_SALON"),
    (246,"COSMETIC_CLIN", "cosmetic\\\\s?clinic|skin\\\\s?clinic|dermatolog|aesthetic|laser\\\\s?clinic|botox",
                                                                      "HEALTH_WELLNESS", "BEAUTY_WELLNESS",   "COSMETIC_CLINIC"),
    (247,"NAIL_BEAUTY",   "nail\\\\s?(salon|spa|bar)|manicure|pedicure|wax(ing)?\\\\b",
                                                                      "HEALTH_WELLNESS", "BEAUTY_WELLNESS",   "BEAUTY_SALON"),

    # ── MALLS / DEPARTMENT STORES (90-99) ────────────────────
    (90, "CENTRAL_MALL",  "central\\\\s?(world|embassy|chidlom|plaza|ladprao|pinklao|rama|westgate|festival|village)",
                                                                      "SHOPPING_RETAIL", "MALLS_DEPARTMENT", "CENTRAL_GROUP_MALLS"),
    (91, "CENTRAL_DEPT",  "central\\\\s?department|central\\\\s?online", "SHOPPING_RETAIL", "MALLS_DEPARTMENT", "CENTRAL_GROUP_MALLS"),
    (92, "EMPORIUM",      "emporium|emquartier|em\\\\s?quarter|em\\\\s?sphere","SHOPPING_RETAIL", "MALLS_DEPARTMENT", "THE_MALL_GROUP"),
    (93, "PARAGON",       "siam\\\\s?paragon|paragon",                  "SHOPPING_RETAIL", "MALLS_DEPARTMENT", "SIAM_EM_DISTRICT"),
    (94, "SIAM_CENTER",   "siam\\\\s?(center|discovery|square)",        "SHOPPING_RETAIL", "MALLS_DEPARTMENT", "SIAM_EM_DISTRICT"),
    (95, "ICONSIAM",      "iconsiam|icon\\\\s?siam",                    "SHOPPING_RETAIL", "MALLS_DEPARTMENT", "ICONSIAM"),
    (96, "MEGA",          "mega\\\\s?(bangna|cineplex|bang)",           "SHOPPING_RETAIL", "MALLS_DEPARTMENT", "MEGA_MALLS"),
    (97, "TERMINAL21",    "terminal\\\\s?21",                           "SHOPPING_RETAIL", "MALLS_DEPARTMENT", "DEPARTMENT_STORES"),
    (98, "ROBINSON",      "robinson",                                 "SHOPPING_RETAIL", "MALLS_DEPARTMENT", "DEPARTMENT_STORES"),
    (99, "THE_MALL",      "the\\\\s?mall(?!\\\\s?group)",                 "SHOPPING_RETAIL", "MALLS_DEPARTMENT", "THE_MALL_GROUP"),
    (100,"PLATINUM_MALL", "platinum\\\\s?(fashion|mall)",               "SHOPPING_RETAIL", "MALLS_DEPARTMENT", "DEPARTMENT_STORES"),

    # ── FASHION / APPAREL (102-114) ──────────────────────────
    (102,"UNIQLO",        "uniqlo",                                   "SHOPPING_RETAIL", "FASHION", "COMMON_FASHION_BRANDS"),
    (103,"HM",            "h&m|h\\\\s?and\\\\s?m",                       "SHOPPING_RETAIL", "FASHION", "COMMON_FASHION_BRANDS"),
    (104,"ZARA",          "\\\\bzara\\\\b",                               "SHOPPING_RETAIL", "FASHION", "COMMON_FASHION_BRANDS"),
    (105,"NIKE",          "\\\\bnike\\\\b",                               "SHOPPING_RETAIL", "FASHION", "SPORTSWEAR"),
    (106,"ADIDAS",        "adidas",                                   "SHOPPING_RETAIL", "FASHION", "SPORTSWEAR"),
    (107,"UNDER_ARMOUR",  "under\\\\s?armour",                          "SHOPPING_RETAIL", "FASHION", "SPORTSWEAR"),
    (108,"LV",            "louis\\\\s?vuitton|\\\\blv\\\\b",               "SHOPPING_RETAIL", "FASHION", "LUXURY_FASHION_BRANDS"),
    (109,"GUCCI",         "gucci",                                    "SHOPPING_RETAIL", "FASHION", "LUXURY_FASHION_BRANDS"),
    (110,"PRADA",         "prada",                                    "SHOPPING_RETAIL", "FASHION", "LUXURY_FASHION_BRANDS"),
    (111,"CHANEL",        "chanel",                                   "SHOPPING_RETAIL", "FASHION", "LUXURY_FASHION_BRANDS"),
    (112,"HERMES",        "hermes|herm.s",                            "SHOPPING_RETAIL", "FASHION", "LUXURY_FASHION_BRANDS"),
    (113,"DIOR",          "\\\\bdior\\\\b",                               "SHOPPING_RETAIL", "FASHION", "LUXURY_FASHION_BRANDS"),
    (114,"BURBERRY",      "burberry",                                 "SHOPPING_RETAIL", "FASHION", "LUXURY_FASHION_BRANDS"),

    # ── ELECTRONICS (116-119) ────────────────────────────────
    (116,"POWERBUY",      "power\\\\s?buy",                             "SHOPPING_RETAIL", "ELECTRONICS", "ELECTRONICS_RETAIL"),
    (117,"IT_CITY",       "it\\\\s?city|banana\\\\s?it|jib\\\\b",          "SHOPPING_RETAIL", "ELECTRONICS", "ELECTRONICS_RETAIL"),
    (118,"STUDIO7",       "studio\\\\s?7|istudio|i\\\\s?studio",         "SHOPPING_RETAIL", "ELECTRONICS", "ELECTRONICS_RETAIL"),

    # ── BOOKS / GIFTS / HOBBY (120-124) ──────────────────────
    (120,"B2S",           "\\\\bb2s\\\\b",                                "SHOPPING_RETAIL", "BOOKS_STATIONERY", "BOOKSTORE_CHAIN"),
    (121,"SEED",          "\\\\bse-?ed\\\\b|se\\\\s?education",            "SHOPPING_RETAIL", "BOOKS_STATIONERY", "BOOKSTORE_CHAIN"),
    (122,"KINOKUNIYA",    "kinokuniya",                               "SHOPPING_RETAIL", "BOOKS_STATIONERY", "BOOKSTORE_CHAIN"),
    (123,"ASIA_BOOKS",    "asia\\\\s?books|naiin",                      "SHOPPING_RETAIL", "BOOKS_STATIONERY", "BOOKSTORE_CHAIN"),
    (124,"LEGO",          "\\\\blego\\\\b",                               "SHOPPING_RETAIL", "GIFTS_HOBBY_TOYS", "TOY_HOBBY"),

    # ── VARIETY / HOME GOODS (125-127) ───────────────────────
    (125,"MRDIY",         "mr\\\\.?\\\\s?d\\\\.?i\\\\.?y",                 "SHOPPING_RETAIL", "VARIETY_HOME",  "VARIETY_STORE"),
    (126,"DAISO",         "daiso",                                    "SHOPPING_RETAIL", "VARIETY_HOME",  "VARIETY_STORE"),

    # ── DINING / QSR / COFFEE (130-149) ──────────────────────
    (130,"MK_REST",       "\\\\bmk\\\\b.*restaurant|mk\\\\s?suki",        "FOOD_DINING", "RESTAURANTS",   "THAI_CHAIN_RESTAURANTS"),
    (131,"SHABUSHI",      "shabushi|oishi|oishi\\\\s?ramen",            "FOOD_DINING", "RESTAURANTS",   "THAI_CHAIN_RESTAURANTS"),
    (132,"SIZZLER",       "sizzler",                                  "FOOD_DINING", "RESTAURANTS",   "INTERNATIONAL_CHAIN"),
    (133,"BBQ_PLAZA",     "bar-?b-?q\\\\s?plaza|bbq\\\\s?plaza",        "FOOD_DINING", "RESTAURANTS",   "THAI_CHAIN_RESTAURANTS"),
    (134,"FUJI",          "\\\\bfuji\\\\b",                               "FOOD_DINING", "RESTAURANTS",   "THAI_CHAIN_RESTAURANTS"),
    (135,"YAYOI",         "yayoi",                                    "FOOD_DINING", "RESTAURANTS",   "THAI_CHAIN_RESTAURANTS"),
    (136,"HAIDILAO",      "haidilao|hai\\\\s?di\\\\s?lao",               "FOOD_DINING", "RESTAURANTS",   "INTERNATIONAL_CHAIN"),
    (137,"SP",            "\\\\bs&p\\\\b|s\\\\s?and\\\\s?p\\\\s?rest",       "FOOD_DINING", "RESTAURANTS",   "THAI_CHAIN_RESTAURANTS"),
    (140,"MCDONALDS",     "mcdonald|mc\\\\s?donald",                   "FOOD_DINING", "FAST_FOOD_QSR", "FAST_FOOD_CHAIN"),
    (141,"KFC",           "\\\\bkfc\\\\b|kentucky",                      "FOOD_DINING", "FAST_FOOD_QSR", "FAST_FOOD_CHAIN"),
    (142,"BURGER_KING",   "burger\\\\s?king",                          "FOOD_DINING", "FAST_FOOD_QSR", "FAST_FOOD_CHAIN"),
    (143,"PIZZA",         "pizza\\\\s?(company|hut)|domino",           "FOOD_DINING", "FAST_FOOD_QSR", "FAST_FOOD_CHAIN"),
    (144,"SUBWAY",        "subway",                                   "FOOD_DINING", "FAST_FOOD_QSR", "FAST_FOOD_CHAIN"),
    (145,"BONCHON",       "bonchon|bon\\\\s?chon",                     "FOOD_DINING", "FAST_FOOD_QSR", "FAST_FOOD_CHAIN"),
    (146,"STARBUCKS",     "starbucks",                                "FOOD_DINING", "COFFEE_CAFE",   "COFFEE_CHAINS"),
    (147,"CAFE_AMAZON",   "caf.\\\\s?amazon|cafe\\\\s?amazon",          "FOOD_DINING", "COFFEE_CAFE",   "COFFEE_CHAINS"),
    (148,"INTHANIN",      "inthanin|true\\\\s?coffee",                 "FOOD_DINING", "COFFEE_CAFE",   "COFFEE_CHAINS"),
    (149,"DUNKIN",        "dunkin|krispy\\\\s?kreme",                  "FOOD_DINING", "BAKERY_DESSERT","BAKERY_CHAIN"),
    (150,"SWENSENS",      "swensen|after\\\\s?you|dairy\\\\s?queen",    "FOOD_DINING", "BAKERY_DESSERT","BAKERY_CHAIN"),

    # ── AIRLINES (155-164) ───────────────────────────────────
    (155,"THAI_AIRWAYS",  "thai\\\\s?airway|thai\\\\s?air(?!asia)",      "TRAVEL_MOBILITY", "AIRLINES", "DOMESTIC_AIRLINE"),
    (156,"BANGKOK_AIR",   "bangkok\\\\s?airway",                       "TRAVEL_MOBILITY", "AIRLINES", "DOMESTIC_AIRLINE"),
    (157,"AIRASIA",       "airasia|air\\\\s?asia",                     "TRAVEL_MOBILITY", "AIRLINES", "DOMESTIC_AIRLINE"),
    (158,"NOKAIR",        "nok\\\\s?air",                              "TRAVEL_MOBILITY", "AIRLINES", "DOMESTIC_AIRLINE"),
    (159,"VIETJET",       "vietjet|viet\\\\s?jet",                     "TRAVEL_MOBILITY", "AIRLINES", "DOMESTIC_AIRLINE"),
    (160,"LION_AIR",      "lion\\\\s?air",                             "TRAVEL_MOBILITY", "AIRLINES", "DOMESTIC_AIRLINE"),
    (161,"SQ",            "singapore\\\\s?airlines|\\\\bsia\\\\b",       "TRAVEL_MOBILITY", "AIRLINES", "INTERNATIONAL_AIRLINE"),
    (162,"EMIRATES",      "emirates",                                 "TRAVEL_MOBILITY", "AIRLINES", "INTERNATIONAL_AIRLINE"),
    (163,"QATAR",         "qatar\\\\s?airway",                         "TRAVEL_MOBILITY", "AIRLINES", "INTERNATIONAL_AIRLINE"),
    (164,"CATHAY",        "cathay\\\\s?pacific",                       "TRAVEL_MOBILITY", "AIRLINES", "INTERNATIONAL_AIRLINE"),

    # ── HOTELS (166-172) ─────────────────────────────────────
    (166,"MARRIOTT",      "marriott|ritz.carlton|w\\\\s?hotel|westin|st\\\\.?\\\\s?regis",
                                                                      "TRAVEL_MOBILITY", "HOTELS", "HOTEL_CHAIN"),
    (167,"HILTON",        "hilton|conrad|waldorf",                    "TRAVEL_MOBILITY", "HOTELS", "HOTEL_CHAIN"),
    (168,"IHG",           "\\\\bihg\\\\b|intercontinental|holiday\\\\s?inn|crowne\\\\s?plaza",
                                                                      "TRAVEL_MOBILITY", "HOTELS", "HOTEL_CHAIN"),
    (169,"ACCOR",         "accor|novotel|ibis|pullman|sofitel|movenpick",
                                                                      "TRAVEL_MOBILITY", "HOTELS", "HOTEL_CHAIN"),
    (170,"HYATT",         "hyatt",                                    "TRAVEL_MOBILITY", "HOTELS", "HOTEL_CHAIN"),
    (171,"CENTARA",       "centara|dusit\\\\s?thani|anantara",         "TRAVEL_MOBILITY", "HOTELS", "BOUTIQUE_HOTEL"),
    (172,"SHERATON",      "sheraton|le\\\\s?meridien|four\\\\s?points",  "TRAVEL_MOBILITY", "HOTELS", "HOTEL_CHAIN"),

    # ── OTA / TRAVEL (175-179) ───────────────────────────────
    (175,"AGODA",         "agoda",                                    "TRAVEL_MOBILITY", "OTA_TRAVEL", "OTA_PLATFORM"),
    (176,"BOOKING_COM",   "booking\\\\.com|booking\\\\s?com",            "TRAVEL_MOBILITY", "OTA_TRAVEL", "OTA_PLATFORM"),
    (177,"TRAVELOKA",     "traveloka",                                "TRAVEL_MOBILITY", "OTA_TRAVEL", "OTA_PLATFORM"),
    (178,"EXPEDIA",       "expedia|hotels\\\\.com",                     "TRAVEL_MOBILITY", "OTA_TRAVEL", "OTA_PLATFORM"),
    (179,"KLOOK",         "klook|getyourguide|kkday",                 "TRAVEL_MOBILITY", "OTA_TRAVEL", "ACTIVITY_BOOKING"),

    # ── HOME IMPROVEMENT / FURNITURE (180-185) ───────────────
    (180,"HOMEPRO",       "homepro|home\\\\s?pro",                     "HOME_LIVING", "HOME_IMPROVEMENT", "HOME_IMPROVEMENT_STORE"),
    (181,"GLOBAL_HSE",    "global\\\\s?house",                         "HOME_LIVING", "HOME_IMPROVEMENT", "HOME_IMPROVEMENT_STORE"),
    (182,"THAIWATSADU",   "thai\\\\s?watsadu|baan\\\\s?&\\\\s?beyond",   "HOME_LIVING", "HOME_IMPROVEMENT", "HOME_IMPROVEMENT_STORE"),
    (183,"DOHOME",        "dohome|do\\\\s?home",                       "HOME_LIVING", "HOME_IMPROVEMENT", "HOME_IMPROVEMENT_STORE"),
    (184,"IKEA",          "ikea",                                     "HOME_LIVING", "FURNITURE_HOME",   "FURNITURE_STORE"),
    (185,"SB_FURNITURE",  "sb\\\\s?furniture|sb\\\\s?design|index\\\\s?living|modernform",
                                                                      "HOME_LIVING", "FURNITURE_HOME",   "FURNITURE_STORE"),

    # ── TELCO (188-192) ──────────────────────────────────────
    (188,"AIS",           "\\\\bais\\\\b|advance\\\\s?info",              "DIGITAL_SERVICES", "TELCO_MOBILE",    "TELCO_MOBILE_PROVIDER"),
    (189,"DTAC",          "\\\\bdtac\\\\b",                               "DIGITAL_SERVICES", "TELCO_MOBILE",    "TELCO_MOBILE_PROVIDER"),
    (190,"TRUEMOVE",      "true\\\\s?(move|corp|online|vision|id)",    "DIGITAL_SERVICES", "TELCO_MOBILE",    "TELCO_MOBILE_PROVIDER"),
    (191,"THREE_BB",      "3bb|3\\\\s?bb|triple\\\\s?t",                "DIGITAL_SERVICES", "TELCO_BROADBAND", "TELCO_BROADBAND_PROVIDER"),
    (192,"TOT",           "\\\\btot\\\\b|tot\\\\s?public",                "DIGITAL_SERVICES", "TELCO_BROADBAND", "TELCO_BROADBAND_PROVIDER"),

    # ── INSURANCE / FINANCE (195-199) ────────────────────────
    (195,"AIA",           "\\\\baia\\\\b",                                "FINANCIAL_SERVICES", "INSURANCE",          "INSURANCE_PAYMENTS"),
    (196,"MUANGTHAI",     "muang\\\\s?thai\\\\s?(life|insurance)",       "FINANCIAL_SERVICES", "INSURANCE",          "INSURANCE_PAYMENTS"),
    (197,"FWD",           "\\\\bfwd\\\\b|fwd\\\\s?insurance",              "FINANCIAL_SERVICES", "INSURANCE",          "INSURANCE_PAYMENTS"),
    (198,"BITKUB",        "bitkub",                                   "FINANCIAL_SERVICES", "INVESTMENT_TRADING", "TRADING_PLATFORM"),

    # ── B2B / GOVERNMENT / PROFESSIONAL (200-206) ────────────
    (200,"GOVERNMENT",    "government|counter\\\\s?service|revenue\\\\s?dept",
                                                                      "B2B_PROFESSIONAL", "GOVERNMENT",            "GOVERNMENT_SERVICE"),
    (201,"OFFICEMATE",    "officemate|office\\\\s?mate|b2s\\\\s?office", "B2B_PROFESSIONAL", "PROFESSIONAL_SERVICES","OFFICE_SUPPLY"),
    (202,"KERRY",         "kerry\\\\s?express|flash\\\\s?express|j&t|best\\\\s?express",
                                                                      "B2B_PROFESSIONAL", "PROFESSIONAL_SERVICES","COURIER_LOGISTICS"),

    # ── ENTERTAINMENT / SPORTS (210-216) ─────────────────────
    (210,"MAJOR_CINEMA",  "major\\\\s?(cineplex|cinema)|sf\\\\s?cinema", "ENTERTAINMENT_LEISURE", "CINEMA",          "CINEMA_CHAIN"),
    (211,"DECATHLON",     "decathlon|super\\\\s?sport",                "ENTERTAINMENT_LEISURE", "SPORTS_ACTIVITY",  "SPECIALITY_SPORT"),
    (212,"GOLF",          "golf\\\\s?(club|course|range|driving)",     "ENTERTAINMENT_LEISURE", "SPORTS_ACTIVITY",  "GOLF"),
    (213,"FITNESS",       "fitness\\\\s?first|virgin\\\\s?active|jetts|anytime\\\\s?fitness",
                                                                      "HEALTH_WELLNESS",       "FITNESS_GYM",      "GYM_FITNESS"),

    # ── EDUCATION (220-222) ──────────────────────────────────
    (220,"EDUCATION",     "university|\\\\buniv\\\\b|school|college|academy",
                                                                      "EDUCATION", "SCHOOL_UNIVERSITY", "UNIVERSITY"),
    (221,"TUTORING",      "tutor|kumon|british\\\\s?council|wall\\\\s?street\\\\s?english",
                                                                      "EDUCATION", "TUTORING_LANGUAGE", "TUTORING_CENTER"),

    # ── AUTOMOTIVE (225-228) ─────────────────────────────────
    (225,"CAR_SERVICE",   "auto\\\\s?service|car\\\\s?care|b-?quik|cockpit|tyreplus",
                                                                      "AUTOMOTIVE", "VEHICLE_SERVICE", "CAR_SERVICE"),
    (226,"CAR_RENTAL",    "hertz|avis|budget\\\\s?rent|thai\\\\s?rent",
                                                                      "AUTOMOTIVE", "CAR_RENTAL",      "CAR_RENTAL_AGENCY"),

    # ── PHARMACY (strictly) (230-232) ────────────────────────
    (230,"PHARMACY",      "pharmacy|drugstore|pharma\\\\s?choice|fascino\\\\s?pharma",
                                                                      "EVERYDAY_ESSENTIALS", "PHARMACY_DRUGSTORE", "PHARMACY_CHAIN"),

    # ── UTILITIES (235-237) ──────────────────────────────────
    (235,"ELECTRIC",      "provincial\\\\s?electric|metropolitan\\\\s?electric|pea\\\\b|mea\\\\b",
                                                                      "EVERYDAY_ESSENTIALS", "UTILITIES", "WATER_ELECTRIC_GAS"),
    (236,"WATER",         "waterworks|water\\\\s?supply|mwa\\\\b|pwa\\\\b",
                                                                      "EVERYDAY_ESSENTIALS", "UTILITIES", "WATER_ELECTRIC_GAS"),

    # ── INCREMENTAL: Utility & government payment gaps ───────
    (248,"GAS_UTILITY",   "ptt\\\\s?gas|\\\\bgas\\\\s?(supply|authority)\\\\b|\\\\bngv\\\\b",
                                                                      "EVERYDAY_ESSENTIALS", "UTILITIES", "WATER_ELECTRIC_GAS"),
    (249,"GOVT_PAYMENT",  "counter\\\\s?service|pay\\\\s?at\\\\s?post|bill\\\\s?payment|\\\\bbill\\\\s?pay\\\\b",
                                                                      "B2B_PROFESSIONAL", "GOVERNMENT", "GOVERNMENT_SERVICE"),
    (250,"INTERNET_ISP",  "\\\\bcat\\\\s?telecom|nt\\\\s?broadband|true\\\\s?internet|\\\\bjasmine\\\\b",
                                                                      "DIGITAL_SERVICES", "TELCO_BROADBAND", "TELCO_BROADBAND_PROVIDER"),

    # ── INCREMENTAL: Pet & veterinary ────────────────────────
    (251,"PET_VET",       "pet\\\\s?(shop|store|hospital)|veterinar|vet\\\\s?clinic|\\\\bpet\\\\b.*mart",
                                                                      "EVERYDAY_ESSENTIALS", "PET_ANIMAL", "PET_CARE"),
]

rule_schema = StructType([
    StructField("priority",      IntegerType(), False),
    StructField("rule_name",     StringType(),  False),
    StructField("regex_pattern", StringType(),  False),
    StructField("rule_l1",       StringType(),  False),
    StructField("rule_l2",       StringType(),  False),
    StructField("rule_l3",       StringType(),  False),
])

merchant_rule_df = spark.createDataFrame(merchant_rules, rule_schema)
print(f"Merchant rules loaded: {merchant_rule_df.count()}")
display(merchant_rule_df.orderBy("priority"))
"""))

# ═══════════════════════════════════════════════════════════════════════════════
# CELL 8: B2K / Mobius Source Framework
# ═══════════════════════════════════════════════════════════════════════════════
cells.append(code("""# COMMAND ----------
# ══════════════════════════════════════════════════════════════
# B2K / MOBIUS SOURCE NORMALIZATION FRAMEWORK
# ══════════════════════════════════════════════════════════════
# Replace these placeholders with your actual governed reference tables.
# Expected: one row per source category code, mapping to our standard taxonomy.

b2k_ref = spark.createDataFrame(
    [],
    "MCHT_CATG string, MCHT_SUB_CATG string, b2k_l1 string, b2k_l2 string, b2k_l3 string"
)

mobius_ref = spark.createDataFrame(
    [],
    "SIC_CD_DESC string, MCC_CD string, mobius_l1 string, mobius_l2 string, mobius_l3 string"
)

# Join source classifications to transactions
txn2 = txn1

# B2K join (left, optional)
if "MCHT_CATG" in txn2.columns:
    txn2 = (
        txn2.alias("t")
        .join(
            F.broadcast(b2k_ref).alias("b"),
            (F.col("t.MCHT_CATG") == F.col("b.MCHT_CATG")) &
            (F.coalesce(F.col("t.MCHT_SUB_CATG"), F.lit("")) == F.coalesce(F.col("b.MCHT_SUB_CATG"), F.lit(""))),
            how="left"
        )
        .select("t.*", "b.b2k_l1", "b.b2k_l2", "b.b2k_l3")
    )
else:
    txn2 = (txn2
        .withColumn("b2k_l1", F.lit(None).cast("string"))
        .withColumn("b2k_l2", F.lit(None).cast("string"))
        .withColumn("b2k_l3", F.lit(None).cast("string"))
    )

# Mobius join (left, optional)
if "SIC_CD_DESC" in txn2.columns and "MCC_CD" in txn2.columns:
    txn2 = (
        txn2.alias("t")
        .join(
            F.broadcast(mobius_ref).alias("m"),
            (F.coalesce(F.col("t.SIC_CD_DESC"), F.lit("")) == F.coalesce(F.col("m.SIC_CD_DESC"), F.lit(""))),
            how="left"
        )
        .select("t.*", "m.mobius_l1", "m.mobius_l2", "m.mobius_l3")
    )
else:
    txn2 = (txn2
        .withColumn("mobius_l1", F.lit(None).cast("string"))
        .withColumn("mobius_l2", F.lit(None).cast("string"))
        .withColumn("mobius_l3", F.lit(None).cast("string"))
    )

print(f"txn2 with source classifications: {len(txn2.columns)} columns")
"""))

# ═══════════════════════════════════════════════════════════════════════════════
# CELL 9: Rule Matching Engine
# ═══════════════════════════════════════════════════════════════════════════════
cells.append(code("""# COMMAND ----------
# ══════════════════════════════════════════════════════════════
# DETERMINISTIC RULE MATCHING ENGINE
# ══════════════════════════════════════════════════════════════
# Cross-join with broadcast rules, filter matches, pick best by priority.
# Uses Window + row_number for deterministic resolution (not F.first).

matched = (
    txn2.alias("t")
    .join(
        F.broadcast(merchant_rule_df).alias("r"),
        F.expr("t.match_text rlike r.regex_pattern"),
        how="left"
    )
    .withColumn(
        "rule_matched",
        F.when(F.col("r.priority").isNotNull(), F.lit(1)).otherwise(F.lit(0))
    )
)

# Count total matches per transaction for audit
match_counts = (
    matched
    .groupBy("_txn_row_id")
    .agg(F.sum("rule_matched").alias("rule_match_cnt"))
)

# Pick best rule using deterministic window (lowest priority wins)
w_rule = Window.partitionBy("_txn_row_id").orderBy(F.col("r.priority").asc_nulls_last())

matched_ranked = (
    matched
    .withColumn("_rule_rank", F.row_number().over(w_rule))
    .filter(
        (F.col("_rule_rank") == 1) |
        (F.col("rule_matched") == 0)  # keep unmatched rows too
    )
    .drop("_rule_rank")
)

# De-duplicate: keep one row per transaction
matched_dedup = (
    matched_ranked
    .dropDuplicates(["_txn_row_id"])
    .join(match_counts, "_txn_row_id", "left")
    .withColumn(
        "rule_conflict_flag",
        F.when(F.col("rule_match_cnt") > 1, F.lit("MULTIPLE_RULES"))
         .when(F.col("rule_match_cnt") == 1, F.lit("SINGLE_RULE"))
         .otherwise(F.lit("NO_RULE"))
    )
    .withColumnRenamed("rule_name", "matched_rule_name")
)

print(f"Matched transactions: {matched_dedup.count()}")
display(matched_dedup.select("mcht_nm_norm", "matched_rule_name", "rule_l1", "rule_l2", "rule_l3", "rule_match_cnt", "rule_conflict_flag").limit(30))
"""))

# ═══════════════════════════════════════════════════════════════════════════════
# CELL 10: Taxonomy Reconciliation
# ═══════════════════════════════════════════════════════════════════════════════
cells.append(code("""# COMMAND ----------
# ══════════════════════════════════════════════════════════════
# TAXONOMY RECONCILIATION (MULTI-SOURCE)
# ══════════════════════════════════════════════════════════════
# Precedence: MERCHANT_RULE > MOBIUS > B2K > UNCLASSIFIED
# Conflicts between Mobius and B2K are flagged for audit.

txn3 = (
    matched_dedup
    # Source used
    .withColumn(
        "taxonomy_source",
        F.when(F.col("rule_l1").isNotNull(), F.lit("MERCHANT_RULE"))
         .when(F.col("mobius_l1").isNotNull(), F.lit("MOBIUS"))
         .when(F.col("b2k_l1").isNotNull(), F.lit("B2K"))
         .otherwise(F.lit("UNCLASSIFIED"))
    )
    # Final taxonomy columns
    .withColumn("tax_l1", F.coalesce("rule_l1", "mobius_l1", "b2k_l1", F.lit("UNCLASSIFIED")))
    .withColumn("tax_l2", F.coalesce("rule_l2", "mobius_l2", "b2k_l2", F.lit("UNCLASSIFIED")))
    .withColumn("tax_l3", F.coalesce("rule_l3", "mobius_l3", "b2k_l3", F.lit("UNCLASSIFIED")))
    # Confidence tag
    .withColumn(
        "taxonomy_confidence",
        F.when(F.col("taxonomy_source") == "MERCHANT_RULE", F.lit("HIGH"))
         .when(F.col("taxonomy_source").isin("MOBIUS", "B2K"), F.lit("MEDIUM"))
         .otherwise(F.lit("LOW"))
    )
    # Match level
    .withColumn(
        "taxonomy_match_level",
        F.when(F.col("tax_l3") != "UNCLASSIFIED", F.lit("L3"))
         .when(F.col("tax_l2") != "UNCLASSIFIED", F.lit("L2"))
         .when(F.col("tax_l1") != "UNCLASSIFIED", F.lit("L1"))
         .otherwise(F.lit("NONE"))
    )
    # Source conflict detection (Mobius vs B2K at L1)
    .withColumn(
        "taxonomy_conflict_flag",
        F.when(
            (F.col("mobius_l1").isNotNull()) & (F.col("b2k_l1").isNotNull()) &
            (F.col("mobius_l1") != F.col("b2k_l1")),
            F.lit("MOBIUS_B2K_DISAGREE")
        ).when(
            (F.col("mobius_l1").isNotNull()) | (F.col("b2k_l1").isNotNull()),
            F.lit("SINGLE_SOURCE")
        ).otherwise(F.lit("NO_SOURCE"))
    )
)

# Audit: taxonomy source coverage
tax_audit = txn3.groupBy("taxonomy_source", "taxonomy_confidence", "taxonomy_match_level").count().orderBy("count", ascending=False)
display(tax_audit)
"""))

# ═══════════════════════════════════════════════════════════════════════════════
# CELL 11: Channel / Geography / Structural Tags
# ═══════════════════════════════════════════════════════════════════════════════
cells.append(code("""# COMMAND ----------
# ══════════════════════════════════════════════════════════════
# CHANNEL / GEOGRAPHY / STRUCTURAL TAGS
# ══════════════════════════════════════════════════════════════

txn4 = (
    txn3
    # Channel tag
    .withColumn(
        "channel_tag",
        F.when(F.col("ONLN_FLAG").isin("Y", "1"), F.lit("ONLINE"))
         .when(F.col("ONLN_FLAG").isin("N", "0"), F.lit("OFFLINE"))
         .when(F.col("match_text").rlike("|".join(IN_APP_PATTERNS)), F.lit("IN_APP"))
         .otherwise(F.lit("UNKNOWN"))
    )
    # Geography tag
    .withColumn(
        "geo_tag",
        F.when(
            F.coalesce(F.col("TXN_CURR_CD"), F.col("CURR_DESC"), F.lit("")).rlike("(?i)thb|baht|764"),
            F.lit("DOMESTIC")
        )
        .when(
            (F.coalesce(F.col("TXN_CURR_CD"), F.col("CURR_DESC"), F.lit("")) != "") &
            (~F.coalesce(F.col("TXN_CURR_CD"), F.col("CURR_DESC"), F.lit("")).rlike("(?i)thb|baht|764")),
            F.lit("CROSS_BORDER")
        )
        .otherwise(F.lit("UNKNOWN"))
    )
    # Wallet / top-up / processor flags
    .withColumn("is_topup",
        F.when(F.col("match_text").rlike("|".join(TOPUP_PATTERNS)), F.lit(1)).otherwise(F.lit(0))
    )
    .withColumn("is_wallet",
        F.when(F.col("match_text").rlike("|".join(WALLET_PATTERNS)), F.lit(1)).otherwise(F.lit(0))
    )
    .withColumn("is_processor_mediated",
        F.when(F.col("match_text").rlike("|".join(PROCESSOR_PATTERNS)), F.lit(1)).otherwise(F.lit(0))
    )
    # Marketplace flag
    .withColumn("is_marketplace",
        F.when(F.col("tax_l2") == "MARKETPLACE_ECOM", F.lit(1)).otherwise(F.lit(0))
    )
    # Mall ecosystem flag
    .withColumn("is_mall_ecosystem",
        F.when(F.col("tax_l2") == "MALLS_DEPARTMENT", F.lit(1)).otherwise(F.lit(0))
    )
    # Premium merchant flag (brand-based)
    .withColumn("is_premium_merchant",
        F.when(
            (F.col("match_text").rlike("|".join(PREMIUM_BRAND_PATTERNS))) |
            (F.col("tax_l3") == "LUXURY_FASHION_BRANDS") |
            (F.col("tax_l3") == "FINE_DINING") |
            (F.col("tax_l3").isin("SIAM_EM_DISTRICT", "ICONSIAM")),
            F.lit(1)
        ).otherwise(F.lit(0))
    )
    # Food delivery vs actual dining split
    .withColumn("is_food_delivery",
        F.when(F.col("match_text").rlike("|".join(FOOD_DELIVERY_PATTERNS)), F.lit(1)).otherwise(F.lit(0))
    )
    # Ride-hailing flag
    .withColumn("is_ride_hailing",
        F.when(F.col("match_text").rlike("|".join(RIDE_HAILING_PATTERNS)), F.lit(1)).otherwise(F.lit(0))
    )
    # Subscription pattern flag (merchant name matches known subscription services)
    .withColumn("is_subscription",
        F.when(F.col("match_text").rlike("|".join(SUBSCRIPTION_PATTERNS)), F.lit(1)).otherwise(F.lit(0))
    )
    # Grocery flag
    .withColumn("is_grocery",
        F.when(F.col("match_text").rlike("|".join(GROCERY_PATTERNS)), F.lit(1)).otherwise(F.lit(0))
    )
    # Risky merchant flag (pattern-based)
    .withColumn("is_risky_merchant",
        F.when(F.col("match_text").rlike("|".join(RISKY_PATTERNS)), F.lit(1)).otherwise(F.lit(0))
    )
    # Mall ecosystem flag (pattern-based, independent of taxonomy)
    .withColumn("is_mall_pattern",
        F.when(F.col("match_text").rlike("|".join(MALL_PATTERNS)), F.lit(1)).otherwise(F.lit(0))
    )
    # ── Platform ecosystem detection (does NOT change taxonomy) ──
    # Identifies which super-app / platform ecosystem a transaction belongs to.
    # Orthogonal to L1/L2/L3 — a GrabFood txn is FOOD_DINING.FOOD_DELIVERY + GRAB_PLATFORM.
    .withColumn("platform_ecosystem",
        F.when(F.col("match_text").rlike("grab|grabfood|grabtaxi|grabcar|grabpay|grab\\\\s?(express|mart|reward)"),
               F.lit("GRAB_PLATFORM"))
         .when(F.col("match_text").rlike("line|lineman|linepay|line\\\\s?(man|pay|sticker|tv|game|webtoon)|rabbit\\\\s?line"),
               F.lit("LINE_PLATFORM"))
         .when(F.col("match_text").rlike("shopee|shopeepay|shopee\\\\s?(pay|mall|food)"),
               F.lit("SHOPEE_PLATFORM"))
         .when(F.col("match_text").rlike("true|truemoney|true\\\\s?(money|move|coffee|id|online|vision|wallet)"),
               F.lit("TRUE_PLATFORM"))
         .when(F.col("match_text").rlike("lazada"),
               F.lit("LAZADA_PLATFORM"))
         .when(F.col("match_text").rlike("robinhood"),
               F.lit("ROBINHOOD_PLATFORM"))
         .when(F.col("match_text").rlike("\\\\bkbank\\\\b|k\\\\s?plus|kplus|k\\\\+"),
               F.lit("KBANK_PLATFORM"))
         .otherwise(F.lit("NONE"))
    )
)

display(txn4.select("mcht_nm_norm", "channel_tag", "geo_tag", "is_topup", "is_wallet",
                     "is_premium_merchant", "is_food_delivery", "is_ride_hailing",
                     "is_subscription", "is_grocery", "is_risky_merchant",
                     "platform_ecosystem").limit(30))
"""))

# ═══════════════════════════════════════════════════════════════════════════════
# CELL 12: Behavioral Transaction Tags
# ═══════════════════════════════════════════════════════════════════════════════
cells.append(code("""# COMMAND ----------
# ══════════════════════════════════════════════════════════════
# BEHAVIORAL TRANSACTION TAGS
# ══════════════════════════════════════════════════════════════

txn5 = (
    txn4
    # Transaction size tier (THB-calibrated)
    .withColumn(
        "txn_size_tier",
        F.when(F.col("txn_amount") < 100,   "MICRO")
         .when(F.col("txn_amount") < 500,   "SMALL")
         .when(F.col("txn_amount") < 2000,  "STANDARD")
         .when(F.col("txn_amount") < 10000, "MID")
         .when(F.col("txn_amount") < 50000, "LARGE")
         .otherwise("PREMIUM")
    )
    # Time pattern (only if timestamp is meaningful, not midnight default)
    .withColumn("hour_of_day", F.hour("txn_ts"))
    .withColumn("has_real_timestamp",
        F.when(
            (F.hour("txn_ts") == 0) & (F.minute("txn_ts") == 0) & (F.second("txn_ts") == 0),
            F.lit(0)  # midnight default — likely no real timestamp
        ).otherwise(F.lit(1))
    )
    .withColumn(
        "time_pattern",
        F.when(F.col("has_real_timestamp") == 0, F.lit("UNKNOWN_TIME"))
         .when((F.col("hour_of_day") >= 6) & (F.col("hour_of_day") < 9),   "MORNING_ROUTINE")
         .when((F.col("hour_of_day") >= 9) & (F.col("hour_of_day") < 12),  "LATE_MORNING")
         .when((F.col("hour_of_day") >= 12) & (F.col("hour_of_day") < 14), "LUNCH")
         .when((F.col("hour_of_day") >= 14) & (F.col("hour_of_day") < 17), "AFTERNOON")
         .when((F.col("hour_of_day") >= 17) & (F.col("hour_of_day") < 22), "EVENING_SOCIAL")
         .otherwise("LATE_NIGHT")
    )
    .withColumn("txn_dow", F.dayofweek("txn_date"))
    .withColumn(
        "weekpart_tag",
        F.when(F.col("txn_dow").isin(1, 7), "WEEKEND").otherwise("WEEKDAY")
    )
    # Day of month (for payday window detection)
    .withColumn("txn_dom", F.dayofmonth("txn_date"))
    .withColumn("payday_window_flag",
        F.when(F.col("txn_dom").between(25, 31) | F.col("txn_dom").between(1, 5), 1).otherwise(0)
    )
    # Business proxy flag (taxonomy-based)
    .withColumn("is_business_proxy",
        F.when(
            (F.col("tax_l1") == "B2B_PROFESSIONAL") |
            (F.col("tax_l2") == "WHOLESALE") |
            (F.col("tax_l3") == "OFFICE_SUPPLY"),
            F.lit(1)
        ).otherwise(F.lit(0))
    )
    # Travel booking signal
    .withColumn("is_travel_booking",
        F.when(
            F.col("tax_l2").isin("AIRLINES", "HOTELS", "OTA_TRAVEL"),
            F.lit(1)
        ).otherwise(F.lit(0))
    )
    # Risk-like merchant flag (only safe taxonomy-based proxies)
    .withColumn("is_risk_adjacent",
        F.when(
            (F.col("is_topup") == 1) &
            (F.col("txn_amount") > 10000),  # large top-ups as risk proxy
            F.lit(1)
        ).otherwise(F.lit(0))
    )
)

display(txn5.select("mcht_nm_norm", "txn_size_tier", "time_pattern", "weekpart_tag",
                     "is_business_proxy", "is_travel_booking", "is_premium_merchant").limit(30))
"""))

# ═══════════════════════════════════════════════════════════════════════════════
# CELL 13: Recurrence / Subscription Tags (Transaction Level)
# ═══════════════════════════════════════════════════════════════════════════════
cells.append(code("""# COMMAND ----------
# ══════════════════════════════════════════════════════════════
# RECURRENCE / SUBSCRIPTION ENGINE (TRANSACTION LEVEL, PIT-SAFE)
# ══════════════════════════════════════════════════════════════
# Uses customer × merchant ordered history with statistical interval analysis.

w_cm = Window.partitionBy("CUST_NUM", "merchant_key").orderBy("txn_date")
w_cm_hist = Window.partitionBy("CUST_NUM", "merchant_key").orderBy("txn_date").rowsBetween(Window.unboundedPreceding, 0)

txn6 = (
    txn5
    # Lag features for same merchant
    .withColumn("prev_txn_dt_merchant",  F.lag("txn_date").over(w_cm))
    .withColumn("prev_txn_amt_merchant", F.lag("txn_amount").over(w_cm))

    # Days since previous visit to same merchant
    .withColumn("days_since_prev_merchant",
        F.when(F.col("prev_txn_dt_merchant").isNotNull(),
               F.datediff(F.col("txn_date"), F.col("prev_txn_dt_merchant")))
    )

    # Amount delta (relative change)
    .withColumn("amt_delta_pct",
        F.when(
            (F.col("prev_txn_amt_merchant").isNotNull()) & (F.col("prev_txn_amt_merchant") > 0),
            F.abs(F.col("txn_amount") - F.col("prev_txn_amt_merchant")) / F.col("prev_txn_amt_merchant")
        )
    )

    # Cumulative visit count to this merchant (PIT-safe: only up to current row)
    .withColumn("merchant_visit_cnt",
        F.count("*").over(w_cm_hist)
    )

    # Rolling interval statistics (PIT-safe: all visits up to now)
    .withColumn("avg_interval_merchant",
        F.avg("days_since_prev_merchant").over(w_cm_hist)
    )
    .withColumn("std_interval_merchant",
        F.stddev_samp("days_since_prev_merchant").over(w_cm_hist)
    )
    .withColumn("cv_interval",
        F.when(
            (F.col("avg_interval_merchant").isNotNull()) & (F.col("avg_interval_merchant") > 0),
            F.coalesce(F.col("std_interval_merchant"), F.lit(0.0)) / F.col("avg_interval_merchant")
        )
    )

    # Average amount stability
    .withColumn("avg_amt_merchant",
        F.avg("txn_amount").over(w_cm_hist)
    )
    .withColumn("std_amt_merchant",
        F.stddev_samp("txn_amount").over(w_cm_hist)
    )
    .withColumn("amt_cv_merchant",
        F.when(
            (F.col("avg_amt_merchant").isNotNull()) & (F.col("avg_amt_merchant") > 0),
            F.coalesce(F.col("std_amt_merchant"), F.lit(0.0)) / F.col("avg_amt_merchant")
        )
    )

    # Novelty tag
    .withColumn("merchant_novelty_tag",
        F.when(F.col("prev_txn_dt_merchant").isNull(), "FIRST_TIME").otherwise("REPEAT")
    )

    # Recurrence classification (improved beyond 25-35 day)
    .withColumn("recurrence_tag",
        F.when(F.col("prev_txn_dt_merchant").isNull(), "FIRST_TIME")
         .when(
             (F.col("merchant_visit_cnt") >= 3) &
             (F.coalesce(F.col("cv_interval"), F.lit(1.0)) < 0.25) &
             (F.coalesce(F.col("amt_cv_merchant"), F.lit(1.0)) < 0.15) &
             (F.col("avg_interval_merchant").between(15, 45)),
             "SUBSCRIPTION_LIKE"
         )
         .when(
             (F.col("merchant_visit_cnt") >= 2) &
             (F.coalesce(F.col("cv_interval"), F.lit(1.0)) < 0.40) &
             (F.col("avg_interval_merchant").between(10, 60)),
             "RECURRING"
         )
         .when(
             (F.col("merchant_visit_cnt") >= 2) &
             (F.col("days_since_prev_merchant").between(1, 90)),
             "EPISODIC"
         )
         .otherwise("IRREGULAR")
    )

    .withColumn("subscription_candidate_flag",
        F.when(F.col("recurrence_tag").isin("SUBSCRIPTION_LIKE", "RECURRING"), 1).otherwise(0)
    )
)

display(txn6.select("CUST_NUM", "mcht_nm_norm", "txn_date", "merchant_visit_cnt",
                     "days_since_prev_merchant", "cv_interval", "recurrence_tag").limit(30))
"""))

# ═══════════════════════════════════════════════════════════════════════════════
# CELL 14: Phase 1 Write + Audit
# ═══════════════════════════════════════════════════════════════════════════════
cells.append(code("""# COMMAND ----------
# ══════════════════════════════════════════════════════════════
# PHASE 1 OUTPUT: ENRICHED TRANSACTION TABLE + AUDIT
# ══════════════════════════════════════════════════════════════

phase1_df = (
    txn6
    .withColumn("_processed_dtm", F.current_timestamp())
    .withColumn("_phase", F.lit("PHASE1"))
)

phase1_df.write.mode("overwrite").format("delta").saveAsTable(PHASE1_OUTPUT_TABLE)

# ── Audit summary ──
phase1_audit = (
    phase1_df.agg(
        F.count("*").alias("total_txns"),
        F.countDistinct("CUST_NUM").alias("unique_customers"),
        F.countDistinct("merchant_key").alias("unique_merchants"),
        F.sum(F.when(F.col("tax_l1") == "UNCLASSIFIED", 1).otherwise(0)).alias("unclassified_cnt"),
        F.sum(F.when(F.col("taxonomy_source") == "MERCHANT_RULE", 1).otherwise(0)).alias("rule_matched_cnt"),
        F.sum(F.when(F.col("taxonomy_source") == "MOBIUS", 1).otherwise(0)).alias("mobius_matched_cnt"),
        F.sum(F.when(F.col("taxonomy_source") == "B2K", 1).otherwise(0)).alias("b2k_matched_cnt"),
        F.sum(F.when(F.col("rule_conflict_flag") == "MULTIPLE_RULES", 1).otherwise(0)).alias("multi_rule_conflicts"),
        F.sum(F.when(F.col("taxonomy_conflict_flag") == "MOBIUS_B2K_DISAGREE", 1).otherwise(0)).alias("source_conflicts"),
    )
    .withColumn("unclassified_rate", F.col("unclassified_cnt") / F.col("total_txns"))
    .withColumn("rule_coverage_rate", F.col("rule_matched_cnt") / F.col("total_txns"))
    .withColumn("audit_ts", F.current_timestamp())
    .withColumn("phase", F.lit("PHASE1"))
)

phase1_audit.write.mode("append").format("delta").saveAsTable(AUDIT_TABLE)
display(phase1_audit)

# Source coverage by L1
print("=== L1 Distribution ===")
display(phase1_df.groupBy("tax_l1").count().orderBy("count", ascending=False))

# Unclassified sample for rule expansion
print("=== Top Unclassified Merchants (for rule expansion) ===")
display(
    phase1_df
    .filter(F.col("tax_l1") == "UNCLASSIFIED")
    .groupBy("mcht_nm_norm").count()
    .orderBy("count", ascending=False)
    .limit(50)
)
"""))

# ═══════════════════════════════════════════════════════════════════════════════
# CELL 15: Phase 2 Audit (markdown)
# ═══════════════════════════════════════════════════════════════════════════════
cells.append(md("""## Phase 2 — Pre-Build Audit Decisions

### What Phase 1 still leaves ambiguous
- Unclassified merchants: coverage depends on rule breadth + B2K/Mobius quality.
- Merchants matching multiple rules: resolved by priority but audit needed.
- Mall tenant vs mall entity: conservative treatment (mall-level L3).

### Feature reliability assessment
- **High confidence:** txn counts, spend amounts, merchant entropy, channel shares, recurrence stats.
- **Medium confidence:** L1/L2 spend shares (dependent on taxonomy coverage), premium merchant flag.
- **Approximate:** business proxy flag, risk-adjacent flag, concentration shock.

### What NOT to use for pricing/risk if classification is weak
- Do not use `tax_l2` shares for pricing if unclassified rate >20%.
- Do not use `is_risk_adjacent` as a standalone risk marker — it is a proxy only.
- Do not infer gambling or high-risk activity — no taxonomy support for it.

### Key decisions
- **Wallet top-ups excluded** from lifestyle spend shares (separate `wallet_spend_share_m`).
- **Recurrence engine** uses CV threshold < 0.25 for subscriptions (stricter than simple interval).
- **Point-in-time safety:** all rolling windows and lags use ordered windows with no future leakage.
"""))

# ═══════════════════════════════════════════════════════════════════════════════
# CELL 16: Customer-Month Base Aggregation
# ═══════════════════════════════════════════════════════════════════════════════
cells.append(code("""# COMMAND ----------
# ══════════════════════════════════════════════════════════════
# CUSTOMER-MONTH BASE AGGREGATION
# ══════════════════════════════════════════════════════════════

phase1 = spark.table(PHASE1_OUTPUT_TABLE)

cust_month_base = (
    phase1
    .groupBy("CUST_NUM", "txn_month")
    .agg(
        # ── Foundation / Control ──
        F.count("*").alias("txn_cnt_m"),
        F.sum("txn_amount").alias("spend_amt_m"),
        F.avg("txn_amount").alias("avg_ticket_m"),
        F.max("txn_amount").alias("max_ticket_m"),
        F.min("txn_amount").alias("min_ticket_m"),
        F.stddev_samp("txn_amount").alias("stddev_ticket_m"),
        F.expr("percentile_approx(txn_amount, 0.5)").alias("median_ticket_m"),
        F.approx_count_distinct("txn_date").alias("active_days_m"),
        F.approx_count_distinct("merchant_key").alias("uniq_merchants_m"),
        F.approx_count_distinct("tax_l1").alias("uniq_tax_l1_m"),
        F.approx_count_distinct("tax_l2").alias("uniq_tax_l2_m"),
        F.approx_count_distinct("tax_l3").alias("uniq_tax_l3_m"),

        # ── Channel spend ──
        F.sum(F.when(F.col("channel_tag") == "ONLINE",  F.col("txn_amount")).otherwise(0.0)).alias("online_spend_m"),
        F.sum(F.when(F.col("channel_tag") == "OFFLINE", F.col("txn_amount")).otherwise(0.0)).alias("offline_spend_m"),
        F.sum(F.when(F.col("channel_tag") == "IN_APP",  F.col("txn_amount")).otherwise(0.0)).alias("inapp_spend_m"),

        # ── Geography spend ──
        F.sum(F.when(F.col("geo_tag") == "DOMESTIC",     F.col("txn_amount")).otherwise(0.0)).alias("domestic_spend_m"),
        F.sum(F.when(F.col("geo_tag") == "CROSS_BORDER", F.col("txn_amount")).otherwise(0.0)).alias("xborder_spend_m"),

        # ── Wallet / top-up / processor ──
        F.sum(F.when(F.col("is_topup") == 1,              F.col("txn_amount")).otherwise(0.0)).alias("topup_spend_m"),
        F.sum(F.when(F.col("is_wallet") == 1,             F.col("txn_amount")).otherwise(0.0)).alias("wallet_spend_m"),
        F.sum(F.when(F.col("is_processor_mediated") == 1, F.col("txn_amount")).otherwise(0.0)).alias("processor_spend_m"),
        F.sum(F.col("is_topup").cast("int")).alias("topup_txn_cnt_m"),
        F.sum(F.col("is_wallet").cast("int")).alias("wallet_txn_cnt_m"),

        # ── Structural counts ──
        F.sum(F.col("is_marketplace").cast("int")).alias("marketplace_txn_cnt_m"),
        F.sum(F.col("is_mall_ecosystem").cast("int")).alias("mall_txn_cnt_m"),
        F.sum(F.col("is_premium_merchant").cast("int")).alias("premium_txn_cnt_m"),
        F.sum(F.col("is_food_delivery").cast("int")).alias("food_delivery_txn_cnt_m"),
        F.sum(F.col("is_business_proxy").cast("int")).alias("business_proxy_txn_cnt_m"),
        F.sum(F.col("is_travel_booking").cast("int")).alias("travel_booking_txn_cnt_m"),
        F.sum(F.col("is_ride_hailing").cast("int")).alias("ride_hailing_txn_cnt_m"),
        F.sum(F.col("is_subscription").cast("int")).alias("subscription_pattern_txn_cnt_m"),
        F.sum(F.col("is_grocery").cast("int")).alias("grocery_txn_cnt_m"),
        F.sum(F.col("is_risky_merchant").cast("int")).alias("risky_txn_cnt_m"),

        # ── Subscription / recurrence ──
        F.sum(F.col("subscription_candidate_flag").cast("int")).alias("subscription_txn_cnt_m"),
        F.sum(F.when(F.col("subscription_candidate_flag") == 1, F.col("txn_amount")).otherwise(0.0)).alias("subscription_spend_m"),
        F.sum(F.when(F.col("merchant_novelty_tag") == "FIRST_TIME", 1).otherwise(0)).alias("new_merchant_cnt_m"),
        F.sum(F.when(F.col("merchant_novelty_tag") == "REPEAT", 1).otherwise(0)).alias("repeat_merchant_txn_cnt_m"),

        # ── Weekend ──
        F.sum(F.when(F.col("weekpart_tag") == "WEEKEND", F.col("txn_amount")).otherwise(0.0)).alias("weekend_spend_m"),
        F.sum(F.when(F.col("weekpart_tag") == "WEEKDAY", F.col("txn_amount")).otherwise(0.0)).alias("weekday_spend_m"),

        # ── Premium spend ──
        F.sum(F.when(F.col("is_premium_merchant") == 1, F.col("txn_amount")).otherwise(0.0)).alias("premium_spend_m"),
        # ── Mall ecosystem spend ──
        F.sum(F.when(F.col("is_mall_ecosystem") == 1, F.col("txn_amount")).otherwise(0.0)).alias("mall_spend_m"),
        # ── Ride-hailing spend ──
        F.sum(F.when(F.col("is_ride_hailing") == 1, F.col("txn_amount")).otherwise(0.0)).alias("ride_hailing_spend_m"),
        # ── Grocery spend (pattern-based) ──
        F.sum(F.when(F.col("is_grocery") == 1, F.col("txn_amount")).otherwise(0.0)).alias("grocery_spend_m"),
        # ── Subscription pattern spend (merchant pattern, not recurrence-detected) ──
        F.sum(F.when(F.col("is_subscription") == 1, F.col("txn_amount")).otherwise(0.0)).alias("subscription_pattern_spend_m"),
        # ── Risky merchant spend ──
        F.sum(F.when(F.col("is_risky_merchant") == 1, F.col("txn_amount")).otherwise(0.0)).alias("risky_spend_m"),
        # ── Business proxy spend ──
        F.sum(F.when(F.col("is_business_proxy") == 1, F.col("txn_amount")).otherwise(0.0)).alias("business_proxy_spend_m"),
        # ── Food delivery spend ──
        F.sum(F.when(F.col("is_food_delivery") == 1, F.col("txn_amount")).otherwise(0.0)).alias("food_delivery_spend_m"),

        # ── Time-of-day spend (only from real timestamps) ──
        F.sum(F.when(F.col("time_pattern") == "MORNING_ROUTINE", F.col("txn_amount")).otherwise(0.0)).alias("morning_spend_m"),
        F.sum(F.when(F.col("time_pattern") == "LUNCH", F.col("txn_amount")).otherwise(0.0)).alias("lunch_spend_m"),
        F.sum(F.when(F.col("time_pattern") == "EVENING_SOCIAL", F.col("txn_amount")).otherwise(0.0)).alias("evening_spend_m"),
        F.sum(F.when(F.col("time_pattern") == "LATE_NIGHT", F.col("txn_amount")).otherwise(0.0)).alias("late_night_spend_m"),

        # ── Payday window spend ──
        F.sum(F.when(F.col("payday_window_flag") == 1, F.col("txn_amount")).otherwise(0.0)).alias("payday_window_spend_m"),
        F.sum(F.when(F.col("payday_window_flag") == 1, 1).otherwise(0)).alias("payday_window_txn_cnt_m"),

        # (first_time_merchant_cnt_m is same as new_merchant_cnt_m above — use new_merchant_cnt_m)
    )

    # ── Derived shares ──
    .withColumn("online_share_m",   F.when(F.col("spend_amt_m") > 0, F.col("online_spend_m")   / F.col("spend_amt_m")).otherwise(0.0))
    .withColumn("offline_share_m",  F.when(F.col("spend_amt_m") > 0, F.col("offline_spend_m")  / F.col("spend_amt_m")).otherwise(0.0))
    .withColumn("inapp_share_m",    F.when(F.col("spend_amt_m") > 0, F.col("inapp_spend_m")    / F.col("spend_amt_m")).otherwise(0.0))
    .withColumn("xborder_share_m",  F.when(F.col("spend_amt_m") > 0, F.col("xborder_spend_m")  / F.col("spend_amt_m")).otherwise(0.0))
    .withColumn("domestic_share_m", F.when(F.col("spend_amt_m") > 0, F.col("domestic_spend_m") / F.col("spend_amt_m")).otherwise(0.0))
    .withColumn("topup_share_m",    F.when(F.col("spend_amt_m") > 0, F.col("topup_spend_m")    / F.col("spend_amt_m")).otherwise(0.0))
    .withColumn("wallet_share_m",   F.when(F.col("spend_amt_m") > 0, F.col("wallet_spend_m")   / F.col("spend_amt_m")).otherwise(0.0))
    .withColumn("premium_share_m",  F.when(F.col("spend_amt_m") > 0, F.col("premium_spend_m")  / F.col("spend_amt_m")).otherwise(0.0))
    .withColumn("mall_share_m",     F.when(F.col("spend_amt_m") > 0, F.col("mall_spend_m")     / F.col("spend_amt_m")).otherwise(0.0))
    .withColumn("weekend_share_m",  F.when(F.col("spend_amt_m") > 0, F.col("weekend_spend_m")  / F.col("spend_amt_m")).otherwise(0.0))
    .withColumn("subscription_share_m", F.when(F.col("spend_amt_m") > 0, F.col("subscription_spend_m") / F.col("spend_amt_m")).otherwise(0.0))
    .withColumn("ride_hailing_share_m", F.when(F.col("spend_amt_m") > 0, F.col("ride_hailing_spend_m") / F.col("spend_amt_m")).otherwise(0.0))
    .withColumn("grocery_share_m",      F.when(F.col("spend_amt_m") > 0, F.col("grocery_spend_m") / F.col("spend_amt_m")).otherwise(0.0))
    .withColumn("food_delivery_share_m",F.when(F.col("spend_amt_m") > 0, F.col("food_delivery_spend_m") / F.col("spend_amt_m")).otherwise(0.0))
    .withColumn("risky_share_m",        F.when(F.col("spend_amt_m") > 0, F.col("risky_spend_m") / F.col("spend_amt_m")).otherwise(0.0))
    .withColumn("business_proxy_share_m", F.when(F.col("spend_amt_m") > 0, F.col("business_proxy_spend_m") / F.col("spend_amt_m")).otherwise(0.0))
    .withColumn("late_night_share_m",   F.when(F.col("spend_amt_m") > 0, F.col("late_night_spend_m") / F.col("spend_amt_m")).otherwise(0.0))
    .withColumn("payday_share_m",       F.when(F.col("spend_amt_m") > 0, F.col("payday_window_spend_m") / F.col("spend_amt_m")).otherwise(0.0))

    # ── Derived intelligence features ──
    # Consumption spend: total spend minus wallet top-ups (true lifestyle spend)
    .withColumn("consumption_spend_m", F.greatest(F.col("spend_amt_m") - F.col("topup_spend_m"), F.lit(0.0)))
    # Transaction intensity per active day
    .withColumn("txn_per_active_day_m",
        F.when(F.col("active_days_m") > 0, F.col("txn_cnt_m").cast("double") / F.col("active_days_m"))
         .otherwise(0.0)
    )
    # Merchant repeat ratio (repeat txns / total txns)
    .withColumn("merchant_repeat_ratio_m",
        F.when(F.col("txn_cnt_m") > 0,
            F.col("repeat_merchant_txn_cnt_m").cast("double") / F.col("txn_cnt_m")
        ).otherwise(0.0)
    )
    # Novelty rate (first-time merchants / unique merchants)
    .withColumn("merchant_novelty_rate_m",
        F.when(F.col("uniq_merchants_m") > 0,
            F.col("new_merchant_cnt_m").cast("double") / F.col("uniq_merchants_m")
        ).otherwise(0.0)
    )
)

print(f"Customer-month base rows: {cust_month_base.count()}")
"""))

# ═══════════════════════════════════════════════════════════════════════════════
# CELL 17: L1 Spend Composition + Dominant Ecosystem
# ═══════════════════════════════════════════════════════════════════════════════
cells.append(code("""# COMMAND ----------
# ══════════════════════════════════════════════════════════════
# L1 SPEND COMPOSITION + DOMINANT ECOSYSTEM
# ══════════════════════════════════════════════════════════════

l1_spend = (
    phase1
    .groupBy("CUST_NUM", "txn_month", "tax_l1")
    .agg(F.sum("txn_amount").alias("l1_spend"))
)

# Pivot L1 spending
l1_pivot = (
    l1_spend
    .groupBy("CUST_NUM", "txn_month")
    .pivot("tax_l1")
    .sum("l1_spend")
    .fillna(0.0)
)

# Rename L1 columns for clarity
l1_rename_map = {}
for c in l1_pivot.columns:
    if c not in ("CUST_NUM", "txn_month"):
        l1_rename_map[c] = f"l1_{c.lower()}_spend_m"

for old_name, new_name in l1_rename_map.items():
    l1_pivot = l1_pivot.withColumnRenamed(old_name, new_name)

cust_month = cust_month_base.join(l1_pivot, ["CUST_NUM", "txn_month"], "left").fillna(0.0)

# Determine dominant & secondary ecosystem
L1_ECOSYSTEMS = [
    "EVERYDAY_ESSENTIALS", "FOOD_DINING", "TRAVEL_MOBILITY", "SHOPPING_RETAIL",
    "HEALTH_WELLNESS", "DIGITAL_SERVICES", "ENTERTAINMENT_LEISURE", "EDUCATION",
    "HOME_LIVING", "AUTOMOTIVE", "FINANCIAL_SERVICES", "B2B_PROFESSIONAL"
]

l1_spend_cols = [f"l1_{e.lower()}_spend_m" for e in L1_ECOSYSTEMS if f"l1_{e.lower()}_spend_m" in cust_month.columns]

if l1_spend_cols:
    struct_expr = "array(" + ",".join([f"struct({c} as spend, '{c}' as l1)" for c in l1_spend_cols]) + ")"
    cust_month = (
        cust_month
        .withColumn("_l1_arr", F.expr(f"sort_array({struct_expr}, false)"))
        .withColumn("dominant_ecosystem",
            F.when(F.size("_l1_arr") >= 1, F.col("_l1_arr")[0]["l1"]).otherwise(F.lit("UNCLASSIFIED"))
        )
        .withColumn("second_ecosystem",
            F.when(F.size("_l1_arr") >= 2, F.col("_l1_arr")[1]["l1"]).otherwise(F.lit("NONE"))
        )
        .withColumn("top_l1_share",
            F.when(
                (F.size("_l1_arr") >= 1) & (F.col("spend_amt_m") > 0),
                F.col("_l1_arr")[0]["spend"] / F.col("spend_amt_m")
            ).otherwise(0.0)
        )
        .drop("_l1_arr")
    )
else:
    cust_month = (cust_month
        .withColumn("dominant_ecosystem", F.lit("UNCLASSIFIED"))
        .withColumn("second_ecosystem", F.lit("NONE"))
        .withColumn("top_l1_share", F.lit(0.0))
    )

# L1 shares for each ecosystem
for col_name in l1_spend_cols:
    share_name = col_name.replace("_spend_m", "_share_m")
    cust_month = cust_month.withColumn(
        share_name,
        F.when(F.col("spend_amt_m") > 0, F.col(col_name) / F.col("spend_amt_m")).otherwise(0.0)
    )

# Category breadth (how many L1 ecosystems >5% of spend)
breadth_expr = F.lit(0)
for col_name in l1_spend_cols:
    breadth_expr = breadth_expr + F.when(
        (F.col("spend_amt_m") > 0) & (F.col(col_name) / F.col("spend_amt_m") > 0.05),
        F.lit(1)
    ).otherwise(F.lit(0))
cust_month = cust_month.withColumn("category_breadth_m", breadth_expr)

# ── Platform ecosystem spend aggregation ──
# Count distinct platforms used and dominant platform per customer-month.
# Uses the platform_ecosystem tag from Cell 11 (orthogonal to L1/L2/L3 taxonomy).
platform_agg = (
    spark.table(PHASE1_OUTPUT_TABLE)
    .filter(F.col("platform_ecosystem") != "NONE")
    .groupBy("CUST_NUM", "txn_month")
    .agg(
        F.countDistinct("platform_ecosystem").alias("platform_count_m"),
        F.sum("txn_amount").alias("platform_spend_m"),
        F.count("*").alias("platform_txn_cnt_m"),
    )
)

# Dominant platform per customer-month (by spend)
w_plat = Window.partitionBy("CUST_NUM", "txn_month").orderBy(F.desc("_plat_spend"))
platform_dominant = (
    spark.table(PHASE1_OUTPUT_TABLE)
    .filter(F.col("platform_ecosystem") != "NONE")
    .groupBy("CUST_NUM", "txn_month", "platform_ecosystem")
    .agg(F.sum("txn_amount").alias("_plat_spend"))
    .withColumn("_pr", F.row_number().over(w_plat))
    .filter(F.col("_pr") == 1)
    .select("CUST_NUM", "txn_month", F.col("platform_ecosystem").alias("dominant_platform"))
)

cust_month = (
    cust_month
    .join(platform_agg, ["CUST_NUM", "txn_month"], "left")
    .join(platform_dominant, ["CUST_NUM", "txn_month"], "left")
    .fillna({"platform_count_m": 0, "platform_spend_m": 0.0, "platform_txn_cnt_m": 0})
    .withColumn("dominant_platform", F.coalesce(F.col("dominant_platform"), F.lit("NONE")))
    .withColumn("platform_share_m",
        F.when(F.col("spend_amt_m") > 0,
               F.col("platform_spend_m") / F.col("spend_amt_m")
        ).otherwise(0.0)
    )
)

display(cust_month.select("CUST_NUM", "txn_month", "dominant_ecosystem", "second_ecosystem",
                           "category_breadth_m", "dominant_platform", "platform_count_m", "platform_share_m").limit(20))
"""))

# ═══════════════════════════════════════════════════════════════════════════════
# CELL 18: L2 Key Category Shares
# ═══════════════════════════════════════════════════════════════════════════════
cells.append(code("""# COMMAND ----------
# ══════════════════════════════════════════════════════════════
# L2 KEY CATEGORY SHARES
# ══════════════════════════════════════════════════════════════
# Selected L2 categories that drive campaign / CLM / pricing decisions.

KEY_L2_CATEGORIES = [
    "GROCERY_MODERN", "CONVENIENCE_STORE", "FUEL", "WHOLESALE",
    "RESTAURANTS", "FAST_FOOD_QSR", "COFFEE_CAFE", "FOOD_DELIVERY",
    "MARKETPLACE_ECOM", "MALLS_DEPARTMENT", "FASHION",
    "STREAMING", "AIRLINES", "HOTELS", "OTA_TRAVEL",
    "HOSPITAL_CLINIC", "BEAUTY_WELLNESS",
    "HOME_IMPROVEMENT", "DIGITAL_WALLET_TOPUP",
    "TELCO_MOBILE", "ELECTRONICS",
]

l2_spend = (
    phase1
    .groupBy("CUST_NUM", "txn_month", "tax_l2")
    .agg(F.sum("txn_amount").alias("l2_spend"))
)

l2_pivot = (
    l2_spend
    .filter(F.col("tax_l2").isin(KEY_L2_CATEGORIES))
    .groupBy("CUST_NUM", "txn_month")
    .pivot("tax_l2")
    .sum("l2_spend")
    .fillna(0.0)
)

# Rename and create shares
for c in l2_pivot.columns:
    if c not in ("CUST_NUM", "txn_month"):
        new_name = f"l2_{c.lower()}_spend_m"
        l2_pivot = l2_pivot.withColumnRenamed(c, new_name)

cust_month = cust_month.join(l2_pivot, ["CUST_NUM", "txn_month"], "left").fillna(0.0)

# Create shares for key L2
for c in [col for col in cust_month.columns if col.startswith("l2_") and col.endswith("_spend_m")]:
    share_name = c.replace("_spend_m", "_share_m")
    cust_month = cust_month.withColumn(
        share_name,
        F.when(F.col("spend_amt_m") > 0, F.col(c) / F.col("spend_amt_m")).otherwise(0.0)
    )
"""))

# ═══════════════════════════════════════════════════════════════════════════════
# CELL 19: Merchant Entropy / HHI / Top3 / Scale
# ═══════════════════════════════════════════════════════════════════════════════
cells.append(code("""# COMMAND ----------
# ══════════════════════════════════════════════════════════════
# MERCHANT ENTROPY / HHI / TOP MERCHANT SHARES
# ══════════════════════════════════════════════════════════════

merchant_month = (
    phase1
    .groupBy("CUST_NUM", "txn_month", "merchant_key")
    .agg(F.sum("txn_amount").alias("merchant_spend_m"))
)

w_cm = Window.partitionBy("CUST_NUM", "txn_month")

merchant_month = (
    merchant_month
    .withColumn("total_spend_cm", F.sum("merchant_spend_m").over(w_cm))
    .withColumn("merchant_share",
        F.when(F.col("total_spend_cm") > 0, F.col("merchant_spend_m") / F.col("total_spend_cm")).otherwise(0.0)
    )
    .withColumn("entropy_component",
        F.when(F.col("merchant_share") > 0, -F.col("merchant_share") * F.log2(F.col("merchant_share"))).otherwise(0.0)
    )
    .withColumn("hhi_component", F.col("merchant_share") ** 2)
)

# Merchant entropy and HHI
merchant_intel = (
    merchant_month
    .groupBy("CUST_NUM", "txn_month")
    .agg(
        F.sum("entropy_component").alias("merchant_entropy_m"),
        F.sum("hhi_component").alias("merchant_hhi_m"),
        F.count("*").alias("merchant_cnt_m"),
    )
)

# Top 1 and Top 3 merchant shares
w_rank = Window.partitionBy("CUST_NUM", "txn_month").orderBy(F.col("merchant_spend_m").desc())

top_merchants = (
    merchant_month
    .withColumn("rk", F.row_number().over(w_rank))
)

top1_share = (
    top_merchants.filter(F.col("rk") == 1)
    .select("CUST_NUM", "txn_month",
            F.col("merchant_share").alias("top1_merchant_share_m"))
)

top3_spend = (
    top_merchants.filter(F.col("rk") <= 3)
    .groupBy("CUST_NUM", "txn_month")
    .agg(F.sum("merchant_spend_m").alias("top3_merchant_spend_m"))
)

cust_month = (
    cust_month
    .join(merchant_intel, ["CUST_NUM", "txn_month"], "left")
    .join(top1_share,     ["CUST_NUM", "txn_month"], "left")
    .join(top3_spend,     ["CUST_NUM", "txn_month"], "left")
    .withColumn("top3_merchant_share_m",
        F.when(F.col("spend_amt_m") > 0, F.col("top3_merchant_spend_m") / F.col("spend_amt_m")).otherwise(0.0)
    )
)

# ── Ecosystem entropy/HHI (on L1 spend shares) ──
l1_month = (
    phase1
    .groupBy("CUST_NUM", "txn_month", "tax_l1")
    .agg(F.sum("txn_amount").alias("l1_spend"))
)

w_l1 = Window.partitionBy("CUST_NUM", "txn_month")

eco_intel = (
    l1_month
    .withColumn("total_l1", F.sum("l1_spend").over(w_l1))
    .withColumn("l1_share", F.when(F.col("total_l1") > 0, F.col("l1_spend") / F.col("total_l1")).otherwise(0.0))
    .withColumn("eco_entropy_c", F.when(F.col("l1_share") > 0, -F.col("l1_share") * F.log2(F.col("l1_share"))).otherwise(0.0))
    .withColumn("eco_hhi_c", F.col("l1_share") ** 2)
    .groupBy("CUST_NUM", "txn_month")
    .agg(
        F.sum("eco_entropy_c").alias("ecosystem_entropy_m"),
        F.sum("eco_hhi_c").alias("ecosystem_hhi_m"),
    )
)

cust_month = cust_month.join(eco_intel, ["CUST_NUM", "txn_month"], "left")
"""))

# ═══════════════════════════════════════════════════════════════════════════════
# CELL 20: Merchant Scale + Loyalty + Novelty + Turnover
# ═══════════════════════════════════════════════════════════════════════════════
cells.append(code("""# COMMAND ----------
# ══════════════════════════════════════════════════════════════
# MERCHANT SCALE / LOYALTY / NOVELTY / TURNOVER
# ══════════════════════════════════════════════════════════════

# ── Merchant scale (PIT: monthly snapshot of merchant size) ──
merchant_monthly_stats = (
    phase1
    .groupBy("merchant_key", "txn_month")
    .agg(
        F.count("*").alias("merchant_global_txn_cnt"),
        F.approx_count_distinct("CUST_NUM").alias("merchant_global_cust_cnt"),
    )
)

merchant_scale_features = (
    phase1
    .select("CUST_NUM", "txn_month", "merchant_key", "txn_amount")
    .join(merchant_monthly_stats, ["merchant_key", "txn_month"], "left")
    .groupBy("CUST_NUM", "txn_month")
    .agg(
        F.avg("merchant_global_txn_cnt").alias("avg_merchant_scale_m"),
        F.sum(F.when(F.col("merchant_global_txn_cnt") >= 100000, F.col("txn_amount")).otherwise(0.0)).alias("spend_at_mega_merchants_m"),
        F.sum(F.when((F.col("merchant_global_txn_cnt") >= 10000) & (F.col("merchant_global_txn_cnt") < 100000), F.col("txn_amount")).otherwise(0.0)).alias("spend_at_large_merchants_m"),
        F.sum(F.when(F.col("merchant_global_txn_cnt") < 1000, F.col("txn_amount")).otherwise(0.0)).alias("spend_at_longtail_merchants_m"),
    )
)

cust_month = cust_month.join(merchant_scale_features, ["CUST_NUM", "txn_month"], "left")

# ── Merchant loyalty / repeat intensity ──
repeat_stats = (
    phase1
    .groupBy("CUST_NUM", "txn_month", "merchant_key")
    .agg(F.count("*").alias("visits_to_merchant"))
    .groupBy("CUST_NUM", "txn_month")
    .agg(
        F.avg("visits_to_merchant").alias("avg_visits_per_merchant_m"),
        F.max("visits_to_merchant").alias("max_visits_per_merchant_m"),
        F.sum(F.when(F.col("visits_to_merchant") >= 3, 1).otherwise(0)).alias("loyal_merchant_cnt_m"),
    )
)

cust_month = cust_month.join(repeat_stats, ["CUST_NUM", "txn_month"], "left")

# ── Merchant turnover (retained / dropped vs prior month) ──
# Unique merchants per customer-month
cust_merchants_this = (
    phase1.select("CUST_NUM", "txn_month", "merchant_key").distinct()
)
cust_merchants_prev = (
    cust_merchants_this
    .withColumn("txn_month_next", F.add_months("txn_month", 1))
    .select(
        F.col("CUST_NUM"),
        F.col("txn_month_next").alias("txn_month"),
        F.col("merchant_key").alias("prev_merchant_key")
    )
)

# Retained = merchants appearing in both months
retained = (
    cust_merchants_this.alias("c")
    .join(
        cust_merchants_prev.alias("p"),
        (F.col("c.CUST_NUM") == F.col("p.CUST_NUM")) &
        (F.col("c.txn_month") == F.col("p.txn_month")) &
        (F.col("c.merchant_key") == F.col("p.prev_merchant_key")),
        how="inner"
    )
    .groupBy(F.col("c.CUST_NUM"), F.col("c.txn_month"))
    .agg(F.countDistinct("c.merchant_key").alias("retained_merchant_cnt_m"))
)

prev_cnt = (
    cust_merchants_prev
    .groupBy("CUST_NUM", "txn_month")
    .agg(F.countDistinct("prev_merchant_key").alias("prev_month_merchant_cnt"))
)

turnover = (
    cust_month.select("CUST_NUM", "txn_month", "uniq_merchants_m", "new_merchant_cnt_m")
    .join(retained, ["CUST_NUM", "txn_month"], "left")
    .join(prev_cnt, ["CUST_NUM", "txn_month"], "left")
    .withColumn("retained_merchant_cnt_m", F.coalesce("retained_merchant_cnt_m", F.lit(0)))
    .withColumn("prev_month_merchant_cnt", F.coalesce("prev_month_merchant_cnt", F.lit(0)))
    .withColumn("dropped_merchant_cnt_m",
        F.greatest(F.col("prev_month_merchant_cnt") - F.col("retained_merchant_cnt_m"), F.lit(0))
    )
    .withColumn("merchant_turnover_rate_m",
        F.when(
            (F.col("uniq_merchants_m") + F.col("prev_month_merchant_cnt")) > 0,
            (F.col("new_merchant_cnt_m") + F.col("dropped_merchant_cnt_m")).cast("double") /
            (F.col("uniq_merchants_m") + F.col("prev_month_merchant_cnt")).cast("double")
        ).otherwise(0.0)
    )
    .withColumn("merchant_churn_rate_m",
        F.when(F.col("prev_month_merchant_cnt") > 0,
            F.col("dropped_merchant_cnt_m").cast("double") / F.col("prev_month_merchant_cnt")
        ).otherwise(0.0)
    )
    .withColumn("merchant_retention_rate_m",
        F.when(F.col("prev_month_merchant_cnt") > 0,
            F.col("retained_merchant_cnt_m").cast("double") / F.col("prev_month_merchant_cnt")
        ).otherwise(F.lit(None))
    )
    .select("CUST_NUM", "txn_month", "retained_merchant_cnt_m", "dropped_merchant_cnt_m",
            "merchant_turnover_rate_m", "merchant_churn_rate_m", "merchant_retention_rate_m")
)

# Need new_merchant_cnt_m already on cust_month
cust_month = cust_month.join(turnover, ["CUST_NUM", "txn_month"], "left")
"""))

# ═══════════════════════════════════════════════════════════════════════════════
# CELL 21: Recurrence / Subscription Features (Customer-Month)
# ═══════════════════════════════════════════════════════════════════════════════
cells.append(code("""# COMMAND ----------
# ══════════════════════════════════════════════════════════════
# RECURRENCE / SUBSCRIPTION FEATURES (CUSTOMER-MONTH GRAIN)
# ══════════════════════════════════════════════════════════════

recurrence_agg = (
    phase1
    .groupBy("CUST_NUM", "txn_month", "merchant_key", "recurrence_tag")
    .agg(
        F.sum("txn_amount").alias("rec_spend"),
        F.count("*").alias("rec_cnt"),
        F.avg("cv_interval").alias("avg_cv"),
        F.avg("avg_interval_merchant").alias("avg_interval"),
        F.avg("amt_cv_merchant").alias("avg_amt_cv"),
    )
)

# Recurring merchant count per customer-month
recurring_summary = (
    recurrence_agg
    .groupBy("CUST_NUM", "txn_month")
    .agg(
        F.sum(F.when(F.col("recurrence_tag") == "SUBSCRIPTION_LIKE", 1).otherwise(0)).alias("subscription_merchant_cnt_m"),
        F.sum(F.when(F.col("recurrence_tag").isin("SUBSCRIPTION_LIKE", "RECURRING"), 1).otherwise(0)).alias("recurring_merchant_cnt_m"),
        F.sum(F.when(F.col("recurrence_tag") == "EPISODIC", 1).otherwise(0)).alias("episodic_merchant_cnt_m"),
        F.sum(F.when(F.col("recurrence_tag") == "IRREGULAR", 1).otherwise(0)).alias("irregular_merchant_cnt_m"),

        F.sum(F.when(F.col("recurrence_tag") == "SUBSCRIPTION_LIKE", F.col("rec_spend")).otherwise(0.0)).alias("subscription_like_spend_m"),
        F.sum(F.when(F.col("recurrence_tag").isin("SUBSCRIPTION_LIKE", "RECURRING"), F.col("rec_spend")).otherwise(0.0)).alias("recurring_spend_m"),

        F.avg(F.when(F.col("recurrence_tag").isin("SUBSCRIPTION_LIKE", "RECURRING"), F.col("avg_interval"))).alias("avg_recurring_interval_m"),
        F.avg(F.when(F.col("recurrence_tag").isin("SUBSCRIPTION_LIKE", "RECURRING"), F.col("avg_cv"))).alias("avg_recurring_cv_m"),
    )
)

cust_month = (
    cust_month
    .join(recurring_summary, ["CUST_NUM", "txn_month"], "left")
    .withColumn("recurring_spend_share_m",
        F.when(F.col("spend_amt_m") > 0, F.coalesce("recurring_spend_m", F.lit(0.0)) / F.col("spend_amt_m")).otherwise(0.0)
    )
    .withColumn("subscription_like_spend_share_m",
        F.when(F.col("spend_amt_m") > 0, F.coalesce("subscription_like_spend_m", F.lit(0.0)) / F.col("spend_amt_m")).otherwise(0.0)
    )
)
"""))

# ═══════════════════════════════════════════════════════════════════════════════
# CELL 22: Wallet / Payment + Rolling Windows
# ═══════════════════════════════════════════════════════════════════════════════
cells.append(code("""# COMMAND ----------
# ══════════════════════════════════════════════════════════════
# WALLET DEPENDENCE + ROLLING WINDOWS (3M / 6M)
# ══════════════════════════════════════════════════════════════

# ── Wallet dependence flags ──
cust_month = (
    cust_month
    .withColumn("wallet_dependence_flag",
        F.when(F.col("wallet_share_m") > 0.50, "HIGH_WALLET")
         .when(F.col("wallet_share_m") > 0.20, "MODERATE_WALLET")
         .otherwise("LOW_WALLET")
    )
    .withColumn("topup_heavy_flag",
        F.when(F.col("topup_share_m") > 0.30, 1).otherwise(0)
    )
    .withColumn("processor_mediated_flag",
        F.when(F.col("spend_amt_m") > 0,
            F.when(F.col("processor_spend_m") / F.col("spend_amt_m") > 0.20, 1).otherwise(0)
        ).otherwise(0)
    )
)

# ── Rolling windows (PIT-safe: only past rows) ──
w_lag = Window.partitionBy("CUST_NUM").orderBy("txn_month")
w_3m  = Window.partitionBy("CUST_NUM").orderBy("txn_month").rowsBetween(-2, 0)
w_6m  = Window.partitionBy("CUST_NUM").orderBy("txn_month").rowsBetween(-5, 0)
w_12m = Window.partitionBy("CUST_NUM").orderBy("txn_month").rowsBetween(-ROLLING_12M, 0)

cust_month = (
    cust_month
    # 3M rolling
    .withColumn("spend_amt_3m",    F.sum("spend_amt_m").over(w_3m))
    .withColumn("txn_cnt_3m",      F.sum("txn_cnt_m").over(w_3m))
    .withColumn("avg_ticket_3m",   F.when(F.col("txn_cnt_3m") > 0, F.col("spend_amt_3m") / F.col("txn_cnt_3m")).otherwise(0.0))
    .withColumn("online_share_3m", F.avg("online_share_m").over(w_3m))
    .withColumn("xborder_share_3m",F.avg("xborder_share_m").over(w_3m))
    .withColumn("wallet_share_3m", F.avg("wallet_share_m").over(w_3m))
    .withColumn("premium_share_3m",F.avg("premium_share_m").over(w_3m))
    .withColumn("merchant_entropy_3m", F.avg("merchant_entropy_m").over(w_3m))
    .withColumn("grocery_share_3m",    F.avg("grocery_share_m").over(w_3m))
    .withColumn("late_night_share_3m", F.avg("late_night_share_m").over(w_3m))
    .withColumn("weekend_share_3m",    F.avg("weekend_share_m").over(w_3m))
    .withColumn("ride_hailing_share_3m", F.avg("ride_hailing_share_m").over(w_3m))
    .withColumn("food_delivery_share_3m", F.avg("food_delivery_share_m").over(w_3m))

    # 6M rolling (selected stability metrics)
    .withColumn("spend_amt_6m",    F.sum("spend_amt_m").over(w_6m))
    .withColumn("txn_cnt_6m",      F.sum("txn_cnt_m").over(w_6m))
    .withColumn("spend_stability_6m",
        F.when(F.avg("spend_amt_m").over(w_6m) > 0,
               F.stddev_samp("spend_amt_m").over(w_6m) / F.avg("spend_amt_m").over(w_6m)
        ).otherwise(F.lit(None))
    )

    # 12M rolling (long-horizon baseline)
    .withColumn("spend_amt_12m",   F.sum("spend_amt_m").over(w_12m))
    .withColumn("txn_cnt_12m",     F.sum("txn_cnt_m").over(w_12m))
    .withColumn("avg_ticket_12m",  F.when(F.col("txn_cnt_12m") > 0, F.col("spend_amt_12m") / F.col("txn_cnt_12m")).otherwise(0.0))
    .withColumn("spend_stability_12m",
        F.when(F.avg("spend_amt_m").over(w_12m) > 0,
               F.stddev_samp("spend_amt_m").over(w_12m) / F.avg("spend_amt_m").over(w_12m)
        ).otherwise(F.lit(None))
    )

    # 3M ecosystem shares (for persona stability)
    .withColumn("dominant_ecosystem_freq_3m",
        F.count("dominant_ecosystem").over(w_3m)  # count of months in window
    )

    # Spend vs 3M average ratio (current month relative to rolling avg)
    .withColumn("spend_vs_3m_avg_ratio",
        F.when(F.avg("spend_amt_m").over(w_3m) > 0,
            F.col("spend_amt_m") / F.avg("spend_amt_m").over(w_3m)
        ).otherwise(F.lit(None))
    )
)
"""))

# ═══════════════════════════════════════════════════════════════════════════════
# CELL 23: Velocity / Trend / Migration
# ═══════════════════════════════════════════════════════════════════════════════
cells.append(code("""# COMMAND ----------
# ══════════════════════════════════════════════════════════════
# VELOCITY / TREND / MIGRATION FEATURES
# ══════════════════════════════════════════════════════════════

cust_month = (
    cust_month
    # ── MoM velocity ──
    .withColumn("spend_prev_m",        F.lag("spend_amt_m").over(w_lag))
    .withColumn("txn_cnt_prev_m",      F.lag("txn_cnt_m").over(w_lag))
    .withColumn("spend_velocity_m",    F.col("spend_amt_m") - F.coalesce("spend_prev_m", F.lit(0.0)))
    .withColumn("spend_pct_change_m",
        F.when((F.col("spend_prev_m").isNotNull()) & (F.col("spend_prev_m") > 0),
               (F.col("spend_amt_m") - F.col("spend_prev_m")) / F.col("spend_prev_m")
        )
    )
    .withColumn("txn_cnt_velocity_m",  F.col("txn_cnt_m") - F.coalesce("txn_cnt_prev_m", F.lit(0)))
    .withColumn("avg_ticket_prev_m",   F.lag("avg_ticket_m").over(w_lag))
    .withColumn("avg_ticket_change_m", F.col("avg_ticket_m") - F.coalesce("avg_ticket_prev_m", F.lit(0.0)))

    # ── Share changes ──
    .withColumn("online_share_prev_m", F.lag("online_share_m").over(w_lag))
    .withColumn("online_share_slope_m",F.col("online_share_m") - F.coalesce("online_share_prev_m", F.lit(0.0)))
    .withColumn("xborder_share_prev_m",F.lag("xborder_share_m").over(w_lag))
    .withColumn("xborder_share_slope_m",F.col("xborder_share_m") - F.coalesce("xborder_share_prev_m", F.lit(0.0)))
    .withColumn("wallet_share_prev_m", F.lag("wallet_share_m").over(w_lag))
    .withColumn("wallet_share_slope_m",F.col("wallet_share_m") - F.coalesce("wallet_share_prev_m", F.lit(0.0)))
    .withColumn("premium_share_prev_m",F.lag("premium_share_m").over(w_lag))
    .withColumn("premium_share_slope_m",F.col("premium_share_m") - F.coalesce("premium_share_prev_m", F.lit(0.0)))

    # ── Acceleration ──
    .withColumn("spend_velocity_prev_m",  F.lag("spend_velocity_m").over(w_lag))
    .withColumn("spend_acceleration_m",   F.col("spend_velocity_m") - F.coalesce("spend_velocity_prev_m", F.lit(0.0)))

    # ── Category migration ──
    .withColumn("dominant_ecosystem_prev_m", F.lag("dominant_ecosystem").over(w_lag))
    .withColumn("l1_migration_flag",
        F.when(
            (F.col("dominant_ecosystem_prev_m").isNotNull()) &
            (F.col("dominant_ecosystem_prev_m") != F.col("dominant_ecosystem")),
            F.lit(1)
        ).otherwise(F.lit(0))
    )

    # ── Entropy trend / diversification ──
    .withColumn("merchant_entropy_prev_m", F.lag("merchant_entropy_m").over(w_lag))
    .withColumn("entropy_trend_m", F.col("merchant_entropy_m") - F.coalesce("merchant_entropy_prev_m", F.lit(0.0)))
    .withColumn("diversification_flag",
        F.when(F.col("entropy_trend_m") > 0.2,  "DIVERSIFYING")
         .when(F.col("entropy_trend_m") < -0.2, "CONCENTRATING")
         .otherwise("STABLE")
    )

    # ── Ecosystem velocity (HHI change = concentration shift) ──
    .withColumn("ecosystem_hhi_prev_m", F.lag("ecosystem_hhi_m").over(w_lag))
    .withColumn("ecosystem_hhi_change_m", F.col("ecosystem_hhi_m") - F.coalesce("ecosystem_hhi_prev_m", F.lit(0.0)))

    # ── Merchant concentration shifts ──
    .withColumn("merchant_hhi_prev_m", F.lag("merchant_hhi_m").over(w_lag))
    .withColumn("merchant_hhi_change_m", F.col("merchant_hhi_m") - F.coalesce("merchant_hhi_prev_m", F.lit(0.0)))
)
"""))

# ═══════════════════════════════════════════════════════════════════════════════
# CELL 24: Regime + Premium/Affluence + Risk + Quality
# ═══════════════════════════════════════════════════════════════════════════════
cells.append(code("""# COMMAND ----------
# ══════════════════════════════════════════════════════════════
# SPEND REGIME + PREMIUM/AFFLUENCE + RISK + FEATURE QUALITY
# ══════════════════════════════════════════════════════════════

cust_month = (
    cust_month
    # ── Spend regime ──
    .withColumn("spend_regime",
        F.when(F.col("txn_cnt_m") == 0, "DORMANT")
         .when(
             (F.col("txn_cnt_m") > 0) & (F.coalesce("txn_cnt_prev_m", F.lit(0)) == 0),
             "REACTIVATING"
         )
         .when(F.col("txn_cnt_m") < 3, "LOW_VOLUME")
         .otherwise("ACTIVE")
    )

    # ── Premium / affluence proxies ──
    .withColumn("premium_retail_orientation",
        F.when(F.col("premium_share_m") > 0.15, "HIGH")
         .when(F.col("premium_share_m") > 0.05, "MODERATE")
         .otherwise("LOW")
    )
    .withColumn("avg_ticket_affluence_proxy",
        F.when(F.col("avg_ticket_m") > 10000, "HIGH_AFFLUENCE_PROXY")
         .when(F.col("avg_ticket_m") > 3000,  "MID_AFFLUENCE_PROXY")
         .otherwise("STANDARD")
    )
    .withColumn("travel_premium_proxy",
        F.when(
            F.coalesce(F.col("l1_travel_mobility_share_m"), F.lit(0.0)) > 0.15,
            "TRAVEL_ORIENTED"
        ).otherwise("NON_TRAVEL")
    )

    # ── Value-seeking / convenience / fuel dependence ──
    .withColumn("convenience_dependence",
        F.when(
            F.coalesce(F.col("l2_convenience_store_share_m"), F.lit(0.0)) > 0.20,
            "HIGH"
        ).when(
            F.coalesce(F.col("l2_convenience_store_share_m"), F.lit(0.0)) > 0.10,
            "MODERATE"
        ).otherwise("LOW")
    )
    .withColumn("fuel_dependence",
        F.when(F.coalesce(F.col("l2_fuel_share_m"), F.lit(0.0)) > 0.15, "HIGH")
         .when(F.coalesce(F.col("l2_fuel_share_m"), F.lit(0.0)) > 0.05, "MODERATE")
         .otherwise("LOW")
    )

    # ── Business proxy signals ──
    .withColumn("business_proxy_flag",
        F.when(
            (F.coalesce(F.col("l1_b2b_professional_share_m"), F.lit(0.0)) > 0.20) |
            (F.col("business_proxy_txn_cnt_m") > F.col("txn_cnt_m") * 0.3),
            "LIKELY_BUSINESS"
        ).when(
            (F.coalesce(F.col("l1_b2b_professional_share_m"), F.lit(0.0)) > 0.10),
            "POSSIBLE_BUSINESS"
        ).otherwise("RETAIL_CONSUMER")
    )

    # ── MONITORING / RISK-LIKE FEATURES (proxies only) ──
    # Concentration shock: sudden increase in merchant HHI
    .withColumn("concentration_shock_flag",
        F.when(
            (F.col("merchant_hhi_change_m") > 0.15) & (F.col("merchant_hhi_m") > 0.5),
            1
        ).otherwise(0)
    )
    # Sudden spend drop
    .withColumn("spend_drop_flag",
        F.when(
            (F.col("spend_pct_change_m").isNotNull()) & (F.col("spend_pct_change_m") < -0.50),
            1
        ).otherwise(0)
    )
    # Category migration shock (changed L1 AND big spend change)
    .withColumn("category_shock_flag",
        F.when(
            (F.col("l1_migration_flag") == 1) &
            (F.abs(F.coalesce("spend_pct_change_m", F.lit(0.0))) > 0.30),
            1
        ).otherwise(0)
    )
    # High velocity low ticket pattern (many small transactions)
    .withColumn("high_freq_low_ticket_flag",
        F.when(
            (F.col("txn_cnt_m") > 30) & (F.col("avg_ticket_m") < 200),
            1
        ).otherwise(0)
    )
    # Wallet/topup overdependence (approximate risk signal)
    .withColumn("topup_overdependence_flag",
        F.when(
            (F.col("topup_share_m") > 0.40) & (F.col("topup_txn_cnt_m") > 5),
            1
        ).otherwise(0)
    )
    # Financial services exposure
    .withColumn("financial_services_share_m",
        F.coalesce(F.col("l1_financial_services_share_m"), F.lit(0.0))
    )
    # Dormant/unstable behavior flag
    .withColumn("unstable_flag",
        F.when(
            (F.col("spend_regime") == "REACTIVATING") |
            (F.col("spend_stability_6m") > 1.5),  # CV > 1.5 = very volatile
            1
        ).otherwise(0)
    )

    # ── FEATURE QUALITY FLAGS ──
    .withColumn("feature_quality_flag",
        F.when(F.col("txn_cnt_m") == 0, "ZERO_SPEND")
         .when(F.col("txn_cnt_m") < 3, "LOW_VOLUME")
         .when(F.col("uniq_merchants_m") <= 1, "SINGLE_MERCHANT")
         .otherwise("GOOD")
    )
    .withColumn("_processed_dtm", F.current_timestamp())
    .withColumn("_phase", F.lit("PHASE2"))
)

# ── Phase 2 write ──
cust_month.write.mode("overwrite").format("delta").saveAsTable(PHASE2_OUTPUT_TABLE)

# ── Phase 2 audit ──
phase2_audit = (
    cust_month.agg(
        F.count("*").alias("total_cust_months"),
        F.countDistinct("CUST_NUM").alias("unique_customers"),
        F.avg("txn_cnt_m").alias("avg_txn_cnt"),
        F.avg("spend_amt_m").alias("avg_spend"),
        F.sum(F.when(F.col("feature_quality_flag") == "GOOD", 1).otherwise(0)).alias("good_quality_cnt"),
        F.sum(F.when(F.col("feature_quality_flag") == "LOW_VOLUME", 1).otherwise(0)).alias("low_vol_cnt"),
        F.sum(F.when(F.col("feature_quality_flag") == "ZERO_SPEND", 1).otherwise(0)).alias("zero_spend_cnt"),
        F.avg("merchant_entropy_m").alias("avg_merchant_entropy"),
        F.avg("merchant_hhi_m").alias("avg_merchant_hhi"),
    )
    .withColumn("audit_ts", F.current_timestamp())
    .withColumn("phase", F.lit("PHASE2"))
)

phase2_audit.write.mode("append").format("delta").saveAsTable(AUDIT_TABLE)
display(phase2_audit)

# Bad null-rate check
print("=== Null Rate Check for Critical Features ===")
critical_features = [
    "txn_cnt_m", "spend_amt_m", "avg_ticket_m", "merchant_entropy_m",
    "online_share_m", "dominant_ecosystem", "spend_regime", "feature_quality_flag"
]
for feat in critical_features:
    if feat in cust_month.columns:
        null_rate = cust_month.filter(F.col(feat).isNull()).count() / max(cust_month.count(), 1)
        print(f"  {feat}: null_rate = {null_rate:.4f}")
"""))

# ═══════════════════════════════════════════════════════════════════════════════
# CELL 25: Phase 3 Audit (markdown)
# ═══════════════════════════════════════════════════════════════════════════════
cells.append(md("""## Phase 3 — Pre-Build Audit Decisions

### Persona design principles
- Personas are **mutually exclusive** — one primary persona per customer-month.
- Based on **rolling 3M features** for stability, not one-month snapshots.
- Must be **interpretable** and **linked to strategy** (campaigns, CLM, pricing).
- Not purely volatile micro-segments — those are separate overlay flags.

### What Phase 2 leaves for persona consideration
- Feature quality flag: personas for `ZERO_SPEND` or `LOW_VOLUME` customers should be `DORMANT`.
- Wallet dependence: should not dominate persona assignment unless it's a defining trait.
- Approximate features (business proxy, risk flags) inform overlays, not primary personas.

### Persona-use case philosophy
- Primary personas drive **strategic segmentation** for campaigns and CLM.
- Micro-segments drive **tactical overlay targeting** (e.g., subscription-heavy for streaming offers).
- Risk flags are for **monitoring and alerting**, not customer marketing.
"""))

# ═══════════════════════════════════════════════════════════════════════════════
# CELL 26: Primary Persona Engine
# ═══════════════════════════════════════════════════════════════════════════════
cells.append(code("""# COMMAND ----------
# ══════════════════════════════════════════════════════════════
# PRIMARY PERSONA ENGINE (10 PERSONAS, ROLLING 3M)
# ══════════════════════════════════════════════════════════════
# Mutually exclusive. Rule-based with deterministic precedence.
# Uses 3M rolling features for stability where available.

feature_mart = spark.table(PHASE2_OUTPUT_TABLE)

# ── Explicit persona dimension scores (0-1 scale, for auditability) ──
persona_df = (
    feature_mart
    .withColumn("score_travel",
        F.least(F.lit(1.0),
            F.coalesce(F.col("l1_travel_mobility_share_m"), F.lit(0.0)) * 2.0 +
            F.coalesce(F.col("xborder_share_3m"), F.lit(0.0)) * 1.5 +
            F.when(F.col("travel_booking_txn_cnt_m") >= 2, 0.3).otherwise(0.0)
        )
    )
    .withColumn("score_affluent",
        F.least(F.lit(1.0),
            F.coalesce(F.col("premium_share_3m"), F.lit(0.0)) * 3.0 +
            F.when(F.col("avg_ticket_3m") > 5000, 0.3).otherwise(0.0) +
            F.when(F.col("avg_ticket_3m") > 10000, 0.2).otherwise(0.0)
        )
    )
    .withColumn("score_digital",
        F.least(F.lit(1.0),
            F.coalesce(F.col("online_share_3m"), F.lit(0.0)) * 1.2 +
            F.coalesce(F.col("l1_digital_services_share_m"), F.lit(0.0)) * 1.5
        )
    )
    .withColumn("score_food_social",
        F.least(F.lit(1.0),
            F.coalesce(F.col("l1_food_dining_share_m"), F.lit(0.0)) * 1.5 +
            F.coalesce(F.col("food_delivery_share_m"), F.lit(0.0)) * 2.0
        )
    )
    .withColumn("score_everyday",
        F.least(F.lit(1.0),
            F.coalesce(F.col("grocery_share_3m"), F.lit(0.0)) * 2.0 +
            F.coalesce(F.col("l1_everyday_essentials_share_m"), F.lit(0.0)) * 1.5
        )
    )
    .withColumn("score_business_proxy",
        F.when(F.col("business_proxy_flag") == "LIKELY_BUSINESS", 1.0).otherwise(
            F.least(F.lit(0.5), F.coalesce(F.col("business_proxy_share_m"), F.lit(0.0)) * 3.0)
        )
    )
    .withColumn("score_mobile_wallet",
        F.least(F.lit(1.0),
            F.coalesce(F.col("wallet_share_3m"), F.lit(0.0)) * 2.0 +
            F.coalesce(F.col("online_share_3m"), F.lit(0.0)) * 0.8
        )
    )
    .withColumn("score_concentration",
        F.least(F.lit(1.0),
            F.coalesce(F.col("top1_merchant_share_m"), F.lit(0.0)) * 1.5 +
            F.coalesce(F.col("merchant_hhi_m"), F.lit(0.0)) * 1.2
        )
    )

    # ── Primary persona assignment (deterministic precedence) ──
    .withColumn("primary_persona",
        # 1. DORMANT — no or near-zero activity
        F.when(
            (F.col("spend_regime") == "DORMANT") | (F.col("feature_quality_flag") == "ZERO_SPEND"),
            "DORMANT_LOW_ACTIVITY"
        )
        # 2. BUSINESS_PROXY — likely business spend on retail card
        .when(
            F.col("business_proxy_flag") == "LIKELY_BUSINESS",
            "BUSINESS_PROXY"
        )
        # 3. TRAVEL_CENTRIC — significant travel orientation
        .when(
            (F.coalesce(F.col("l1_travel_mobility_share_m"), F.lit(0.0)) > 0.20) |
            (F.col("xborder_share_3m") > 0.15) |
            (F.col("travel_booking_txn_cnt_m") >= 2),
            "TRAVEL_CENTRIC"
        )
        # 4. AFFLUENT_LIFESTYLE — premium retail + high ticket
        .when(
            (F.col("premium_share_3m") > 0.10) &
            (F.col("avg_ticket_3m") > 5000),
            "AFFLUENT_LIFESTYLE"
        )
        # 5. DIGITAL_NATIVE — digital-first consumption
        .when(
            (F.col("online_share_3m") > 0.55) |
            (F.coalesce(F.col("l1_digital_services_share_m"), F.lit(0.0)) > 0.25),
            "DIGITAL_NATIVE"
        )
        # 6. LIFESTYLE_SOCIAL — dining/food/entertainment oriented
        .when(
            (F.coalesce(F.col("l1_food_dining_share_m"), F.lit(0.0)) > 0.25) |
            (F.coalesce(F.col("l2_restaurants_share_m"), F.lit(0.0)) +
             F.coalesce(F.col("l2_food_delivery_share_m"), F.lit(0.0)) +
             F.coalesce(F.col("l2_coffee_cafe_share_m"), F.lit(0.0)) > 0.25),
            "LIFESTYLE_SOCIAL"
        )
        # 7. CONCENTRATED_LOYALIST — high merchant concentration
        .when(
            (F.coalesce(F.col("top1_merchant_share_m"), F.lit(0.0)) > 0.50) |
            (F.coalesce(F.col("merchant_hhi_m"), F.lit(0.0)) > 0.35),
            "CONCENTRATED_LOYALIST"
        )
        # 8. EMERGING_MOBILE — moderate digital + wallet dependent
        .when(
            (F.col("wallet_share_3m") > 0.20) &
            (F.col("online_share_3m") > 0.30),
            "EMERGING_MOBILE"
        )
        # 9. DIVERSIFIED_EXPLORER — broad category breadth
        .when(
            (F.coalesce(F.col("category_breadth_m"), F.lit(0)) >= 5) &
            (F.coalesce(F.col("merchant_entropy_3m"), F.lit(0.0)) > 2.5),
            "DIVERSIFIED_EXPLORER"
        )
        # 10. EVERYDAY_ANCHOR — default for everyday essential-focused
        .otherwise("EVERYDAY_ANCHOR")
    )
)

# Persona distribution
print("=== Primary Persona Distribution ===")
display(persona_df.groupBy("primary_persona").count().orderBy("count", ascending=False))
"""))

# ═══════════════════════════════════════════════════════════════════════════════
# CELL 27: Micro-Segments / Overlays
# ═══════════════════════════════════════════════════════════════════════════════
cells.append(code("""# COMMAND ----------
# ══════════════════════════════════════════════════════════════
# MICRO-SEGMENT OVERLAYS (NON-EXCLUSIVE FLAGS)
# ══════════════════════════════════════════════════════════════

persona_df = (
    persona_df
    .withColumn("ms_subscription_heavy",
        F.when(F.coalesce(F.col("subscription_merchant_cnt_m"), F.lit(0)) >= 2, 1).otherwise(0)
    )
    .withColumn("ms_wallet_dependent",
        F.when(F.col("wallet_share_3m") > 0.25, 1).otherwise(0)
    )
    .withColumn("ms_food_delivery_native",
        F.when(
            (F.col("food_delivery_txn_cnt_m") >= 3) |
            (F.coalesce(F.col("l2_food_delivery_share_m"), F.lit(0.0)) > 0.10),
            1
        ).otherwise(0)
    )
    .withColumn("ms_cross_border_frequent",
        F.when(F.col("xborder_share_3m") > 0.10, 1).otherwise(0)
    )
    .withColumn("ms_weekend_spender",
        F.when(F.col("weekend_share_m") > 0.45, 1).otherwise(0)
    )
    .withColumn("ms_healthcare_engaged",
        F.when(
            F.coalesce(F.col("l2_hospital_clinic_share_m"), F.lit(0.0)) +
            F.coalesce(F.col("l2_beauty_wellness_share_m"), F.lit(0.0)) > 0.10,
            1
        ).otherwise(0)
    )
    .withColumn("ms_premium_retail_oriented",
        F.when(F.col("premium_share_3m") > 0.08, 1).otherwise(0)
    )
    .withColumn("ms_convenience_dependent",
        F.when(F.col("convenience_dependence") == "HIGH", 1).otherwise(0)
    )
    .withColumn("ms_installment_buyer_proxy",
        F.when(
            (F.col("avg_ticket_m") > 5000) &
            (F.col("txn_cnt_m") <= 5) &
            (F.col("max_ticket_m") > 10000),
            1
        ).otherwise(0)
    )
    .withColumn("ms_fuel_dependent",
        F.when(F.col("fuel_dependence") == "HIGH", 1).otherwise(0)
    )
    .withColumn("ms_reactivating_user",
        F.when(F.col("spend_regime") == "REACTIVATING", 1).otherwise(0)
    )
    .withColumn("ms_business_spend_proxy",
        F.when(F.col("business_proxy_flag").isin("LIKELY_BUSINESS", "POSSIBLE_BUSINESS"), 1).otherwise(0)
    )
    .withColumn("ms_risk_monitor_flag",
        F.when(
            (F.col("concentration_shock_flag") == 1) |
            (F.col("spend_drop_flag") == 1) |
            (F.col("topup_overdependence_flag") == 1) |
            (F.col("high_freq_low_ticket_flag") == 1) |
            (F.col("unstable_flag") == 1),
            1
        ).otherwise(0)
    )
    .withColumn("ms_grocery_anchor",
        F.when(F.coalesce(F.col("grocery_share_m"), F.lit(0.0)) >= 0.30, 1).otherwise(0)
    )
    .withColumn("ms_payday_spender",
        F.when(F.coalesce(F.col("payday_share_m"), F.lit(0.0)) >= 0.50, 1).otherwise(0)
    )
    .withColumn("ms_ride_hailing_regular",
        F.when(F.coalesce(F.col("ride_hailing_share_m"), F.lit(0.0)) >= 0.10, 1).otherwise(0)
    )
)
"""))

# ═══════════════════════════════════════════════════════════════════════════════
# CELL 28: Persona Confidence + Use-Case Views
# ═══════════════════════════════════════════════════════════════════════════════
cells.append(code("""# COMMAND ----------
# ══════════════════════════════════════════════════════════════
# PERSONA CONFIDENCE + SHIFT + USE-CASE LINKAGE
# ══════════════════════════════════════════════════════════════

w_persona = Window.partitionBy("CUST_NUM").orderBy("txn_month")

persona_df = (
    persona_df
    # Persona confidence — based on feature quality and volume
    .withColumn("persona_confidence",
        F.when(F.col("feature_quality_flag") == "GOOD", F.lit("HIGH"))
         .when(F.col("feature_quality_flag") == "LOW_VOLUME", F.lit("MEDIUM"))
         .otherwise(F.lit("LOW"))
    )
    # Numeric confidence score (0-1) for downstream ML / ranking
    .withColumn("persona_confidence_score",
        F.when(F.col("feature_quality_flag") == "GOOD", F.lit(1.0))
         .when(F.col("feature_quality_flag") == "LOW_VOLUME", F.lit(0.5))
         .otherwise(F.lit(0.2))
    )
    # Persona shift detection
    .withColumn("primary_persona_prev_m", F.lag("primary_persona").over(w_persona))
    .withColumn("persona_shift_flag",
        F.when(
            (F.col("primary_persona_prev_m").isNotNull()) &
            (F.col("primary_persona_prev_m") != F.col("primary_persona")),
            1
        ).otherwise(0)
    )
    # Persona tenure (consecutive months with same persona)
    .withColumn("_persona_change",
        F.when(
            (F.col("primary_persona_prev_m").isNull()) |
            (F.col("primary_persona_prev_m") != F.col("primary_persona")),
            F.lit(1)
        ).otherwise(F.lit(0))
    )
    .withColumn("_persona_group", F.sum("_persona_change").over(w_persona))
    .withColumn("persona_tenure_months",
        F.row_number().over(
            Window.partitionBy("CUST_NUM", "_persona_group").orderBy("txn_month")
        )
    )
    .drop("_persona_change", "_persona_group")

    # ── USE-CASE LINKAGE ──
    .withColumn("campaign_use_case",
        F.when(F.col("primary_persona") == "TRAVEL_CENTRIC",
               "travel_benefits|fx_campaigns|premium_card|airline_hotel_partners")
         .when(F.col("primary_persona") == "DIGITAL_NATIVE",
               "digital_activation|bnpl|streaming_bundles|wallet_integration")
         .when(F.col("primary_persona") == "EVERYDAY_ANCHOR",
               "cashback|grocery_convenience_offers|line_protection|essentials")
         .when(F.col("primary_persona") == "LIFESTYLE_SOCIAL",
               "dining_offers|weekend_campaigns|merchant_partnerships|food_delivery")
         .when(F.col("primary_persona") == "AFFLUENT_LIFESTYLE",
               "premium_upgrade|luxury_merchant_offers|exclusive_events|travel_premium")
         .when(F.col("primary_persona") == "BUSINESS_PROXY",
               "sme_proxy_products|working_capital|separate_from_retail_analytics")
         .when(F.col("primary_persona") == "CONCENTRATED_LOYALIST",
               "merchant_specific_rewards|category_diversification|cross_sell")
         .when(F.col("primary_persona") == "EMERGING_MOBILE",
               "mobile_first_offers|wallet_bundles|digital_onboarding")
         .when(F.col("primary_persona") == "DIVERSIFIED_EXPLORER",
               "lifestyle_rewards|broad_merchant_network|premium_migration")
         .when(F.col("primary_persona") == "DORMANT_LOW_ACTIVITY",
               "reactivation|spend_stimulation|dormant_recovery")
         .otherwise("general_engagement")
    )

    .withColumn("clm_use_case",
        F.when(
            (F.col("spend_velocity_m") > 0) & (F.col("avg_ticket_m") > 3000) &
            (F.col("persona_confidence") == "HIGH"),
            "line_increase_candidate"
        )
        .when(
            (F.col("recurring_spend_share_m") > 0.30) & (F.col("spend_regime") == "ACTIVE"),
            "stable_recurring_spend|line_confidence"
        )
        .when(
            (F.col("primary_persona").isin("AFFLUENT_LIFESTYLE", "TRAVEL_CENTRIC")) &
            (F.col("premium_share_3m") > 0.10),
            "premium_migration_candidate"
        )
        .when(
            (F.col("primary_persona") == "DIVERSIFIED_EXPLORER") & (F.col("category_breadth_m") >= 5),
            "diversified_healthy_spend|affluent_migration"
        )
        .otherwise("standard_review")
    )

    .withColumn("pricing_use_case",
        F.when(F.col("xborder_share_3m") > 0.15,           "cross_border_pricing_relevant")
         .when(F.col("primary_persona") == "TRAVEL_CENTRIC","travel_card_pricing")
         .when(F.col("ms_subscription_heavy") == 1,         "subscription_card_features")
         .when(F.col("ms_wallet_dependent") == 1,           "wallet_integration_pricing")
         .when(F.col("ms_premium_retail_oriented") == 1,    "premium_product_pricing")
         .otherwise("standard_pricing")
    )

    .withColumn("risk_use_case",
        F.when(F.col("ms_risk_monitor_flag") == 1,
            F.concat_ws("|",
                F.when(F.col("concentration_shock_flag") == 1, F.lit("concentration_shock")),
                F.when(F.col("spend_drop_flag") == 1, F.lit("spend_decline")),
                F.when(F.col("topup_overdependence_flag") == 1, F.lit("topup_overdependence")),
                F.when(F.col("high_freq_low_ticket_flag") == 1, F.lit("high_freq_low_ticket")),
                F.when(F.col("unstable_flag") == 1, F.lit("unstable_behavior")),
            )
        ).otherwise("no_risk_signal")
    )

    .withColumn("_processed_dtm", F.current_timestamp())
    .withColumn("_phase", F.lit("PHASE3"))
)
"""))

# ═══════════════════════════════════════════════════════════════════════════════
# CELL 29: Final Write + Pipeline Summary
# ═══════════════════════════════════════════════════════════════════════════════
cells.append(code("""# COMMAND ----------
# ══════════════════════════════════════════════════════════════
# FINAL OUTPUT WRITE + PIPELINE SUMMARY
# ══════════════════════════════════════════════════════════════

persona_df.write.mode("overwrite").format("delta").saveAsTable(PHASE3_OUTPUT_TABLE)

# ── Phase 3 audit ──
phase3_audit = (
    persona_df.agg(
        F.count("*").alias("total_cust_months"),
        F.countDistinct("CUST_NUM").alias("unique_customers"),
        # Persona distribution
        F.sum(F.when(F.col("primary_persona") == "EVERYDAY_ANCHOR", 1).otherwise(0)).alias("persona_everyday"),
        F.sum(F.when(F.col("primary_persona") == "DIGITAL_NATIVE", 1).otherwise(0)).alias("persona_digital"),
        F.sum(F.when(F.col("primary_persona") == "LIFESTYLE_SOCIAL", 1).otherwise(0)).alias("persona_lifestyle"),
        F.sum(F.when(F.col("primary_persona") == "TRAVEL_CENTRIC", 1).otherwise(0)).alias("persona_travel"),
        F.sum(F.when(F.col("primary_persona") == "AFFLUENT_LIFESTYLE", 1).otherwise(0)).alias("persona_affluent"),
        F.sum(F.when(F.col("primary_persona") == "EMERGING_MOBILE", 1).otherwise(0)).alias("persona_emerging"),
        F.sum(F.when(F.col("primary_persona") == "BUSINESS_PROXY", 1).otherwise(0)).alias("persona_business"),
        F.sum(F.when(F.col("primary_persona") == "CONCENTRATED_LOYALIST", 1).otherwise(0)).alias("persona_loyalist"),
        F.sum(F.when(F.col("primary_persona") == "DIVERSIFIED_EXPLORER", 1).otherwise(0)).alias("persona_explorer"),
        F.sum(F.when(F.col("primary_persona") == "DORMANT_LOW_ACTIVITY", 1).otherwise(0)).alias("persona_dormant"),
        # Shift rate
        F.avg("persona_shift_flag").alias("avg_persona_shift_rate"),
        # Micro-segment prevalence
        F.sum("ms_risk_monitor_flag").alias("risk_flagged_cnt"),
    )
    .withColumn("audit_ts", F.current_timestamp())
    .withColumn("phase", F.lit("PHASE3"))
)

phase3_audit.write.mode("append").format("delta").saveAsTable(AUDIT_TABLE)
display(phase3_audit)

# ── Campaign-ready output view ──
print("=== Campaign / CLM / Pricing / Risk Ready View ===")
campaign_view = (
    persona_df
    .select(
        "CUST_NUM", "txn_month",
        # Persona
        "primary_persona", "persona_confidence", "persona_confidence_score", "persona_shift_flag", "persona_tenure_months",
        # Use cases
        "campaign_use_case", "clm_use_case", "pricing_use_case", "risk_use_case",
        # Key features
        "spend_amt_m", "consumption_spend_m", "txn_cnt_m", "avg_ticket_m", "max_ticket_m",
        "txn_per_active_day_m",
        "online_share_m", "xborder_share_m", "wallet_share_m", "premium_share_m",
        "merchant_entropy_m", "merchant_hhi_m", "top3_merchant_share_m",
        "merchant_repeat_ratio_m", "merchant_novelty_rate_m",
        "merchant_churn_rate_m", "merchant_retention_rate_m",
        "spend_velocity_m", "spend_acceleration_m",
        "dominant_ecosystem", "second_ecosystem", "category_breadth_m",
        "dominant_platform", "platform_count_m", "platform_share_m",
        "l1_migration_flag", "diversification_flag",
        "spend_regime", "feature_quality_flag",
        # Rolling
        "spend_amt_3m", "avg_ticket_3m", "online_share_3m",
        "spend_vs_3m_avg_ratio",
        "spend_amt_12m", "txn_cnt_12m", "spend_stability_12m",
        "grocery_share_3m", "late_night_share_3m", "weekend_share_3m",
        # Persona scores
        "score_travel", "score_affluent", "score_digital",
        "score_food_social", "score_everyday", "score_business_proxy",
        "score_mobile_wallet", "score_concentration",
        # Micro-segments
        "ms_subscription_heavy", "ms_wallet_dependent", "ms_food_delivery_native",
        "ms_cross_border_frequent", "ms_weekend_spender", "ms_healthcare_engaged",
        "ms_premium_retail_oriented", "ms_convenience_dependent",
        "ms_installment_buyer_proxy", "ms_fuel_dependent",
        "ms_reactivating_user", "ms_business_spend_proxy", "ms_risk_monitor_flag",
        "ms_grocery_anchor", "ms_payday_spender", "ms_ride_hailing_regular",
        # Risk detail
        "concentration_shock_flag", "spend_drop_flag", "topup_overdependence_flag",
        "wallet_dependence_flag", "business_proxy_flag",
    )
)

display(campaign_view.limit(50))
"""))

# ═══════════════════════════════════════════════════════════════════════════════
# CELL 30: QA — Persona Distribution + Taxonomy Coverage
# ═══════════════════════════════════════════════════════════════════════════════
cells.append(code("""# COMMAND ----------
# ══════════════════════════════════════════════════════════════
# QA: PERSONA DISTRIBUTION + TAXONOMY COVERAGE
# ══════════════════════════════════════════════════════════════

final = spark.table(PHASE3_OUTPUT_TABLE)

# ── Persona distribution ──
print("=== Persona Distribution ===")
persona_dist = final.groupBy("primary_persona").agg(
    F.count("*").alias("customer_months"),
    F.round(F.avg("spend_amt_m"), 2).alias("avg_spend"),
    F.round(F.avg("txn_cnt_m"), 1).alias("avg_txn_cnt"),
    F.round(F.avg("avg_ticket_m"), 2).alias("avg_ticket"),
    F.round(F.avg(F.when(F.col("persona_confidence") == "HIGH", 1.0).when(F.col("persona_confidence") == "MEDIUM", 0.5).otherwise(0.2)), 3).alias("avg_confidence"),
)
display(persona_dist.orderBy("customer_months", ascending=False))

# ── Taxonomy coverage ──
print("\\n=== Taxonomy Source Coverage ===")
phase1 = spark.table(PHASE1_OUTPUT_TABLE)
taxonomy_coverage = phase1.groupBy("taxonomy_source").agg(
    F.count("*").alias("txn_count"),
    F.sum("txn_amount").alias("total_spend"),
    F.round(F.sum("txn_amount") / phase1.agg(F.sum("txn_amount")).first()[0] * 100, 2).alias("spend_pct")
)
display(taxonomy_coverage.orderBy("txn_count", ascending=False))

# ── Top unclassified merchants ──
print("\\n=== Top 20 Unclassified Merchants (by spend) ===")
unclassified = (
    phase1
    .filter(F.col("taxonomy_source") == "UNCLASSIFIED")
    .groupBy("mcht_nm_norm")
    .agg(
        F.count("*").alias("txn_count"),
        F.sum("txn_amount").alias("total_spend"),
        F.countDistinct("CUST_NUM").alias("unique_customers")
    )
    .orderBy("total_spend", ascending=False)
    .limit(20)
)
display(unclassified)
"""))

# ═══════════════════════════════════════════════════════════════════════════════
# CELL 31: QA — Feature Mart Health Check
# ═══════════════════════════════════════════════════════════════════════════════
cells.append(code("""# COMMAND ----------
# ══════════════════════════════════════════════════════════════
# QA: FEATURE MART HEALTH CHECK
# ══════════════════════════════════════════════════════════════

# ── Key feature distributions ──
print("=== Feature Mart Key Distributions ===")
feature_stats = final.select(
    F.count("*").alias("total_rows"),
    F.countDistinct("CUST_NUM").alias("unique_customers"),
    F.countDistinct("txn_month").alias("months_covered"),
    F.round(F.avg("spend_amt_m"), 2).alias("avg_spend_m"),
    F.round(F.avg("txn_cnt_m"), 1).alias("avg_txn_cnt"),
    F.round(F.avg("merchant_entropy_m"), 3).alias("avg_merchant_entropy"),
    F.round(F.avg("online_share_m"), 3).alias("avg_online_share"),
    F.round(F.avg("wallet_share_m"), 3).alias("avg_wallet_share"),
)
display(feature_stats)

# ── Null rate audit for critical features ──
print("\\n=== Null Rate Audit (top features) ===")
critical_cols = [
    "spend_amt_m", "txn_cnt_m", "avg_ticket_m", "merchant_entropy_m",
    "online_share_m", "wallet_share_m", "dominant_ecosystem",
    "primary_persona", "persona_confidence", "spend_regime",
    "spend_amt_3m", "spend_amt_12m",
    "grocery_share_m", "late_night_share_m", "payday_share_m",
]
null_checks = final.select(
    *[F.round(F.sum(F.when(F.col(c).isNull(), 1).otherwise(0)) / F.count("*") * 100, 2).alias(c)
      for c in critical_cols if c in final.columns]
)
display(null_checks)

# ── Micro-segment activation rates ──
print("\\n=== Micro-Segment Activation Rates ===")
ms_cols = [c for c in final.columns if c.startswith("ms_")]
ms_rates = final.select(
    *[F.round(F.avg(F.col(c)) * 100, 2).alias(c) for c in ms_cols]
)
display(ms_rates)

# ── Persona score correlation summary ──
print("\\n=== Average Persona Scores by Primary Persona ===")
score_cols = [c for c in final.columns if c.startswith("score_")]
if score_cols:
    score_summary = final.groupBy("primary_persona").agg(
        *[F.round(F.avg(c), 3).alias(c) for c in score_cols]
    )
    display(score_summary.orderBy("primary_persona"))
"""))

# ═══════════════════════════════════════════════════════════════════════════════
# CELL 32: Final Audit Summary (markdown)
# ═══════════════════════════════════════════════════════════════════════════════
cells.append(md("""## Final Audit Summary

### What this notebook includes
| Component | Coverage |
|---|---|
| **Merchant normalization** | Multi-step: lowercase, whitespace, noise symbols, branch/city removal, SHA-256 key |
| **Token extraction** | Split tokens, brand root, token count |
| **Taxonomy hierarchy** | 13 L1 × 45 L2 × 75+ L3 archetypes |
| **Deterministic rules** | ~135 Thailand-specific merchant regex rules with priority |
| **Source reconciliation** | MERCHANT_RULE > MOBIUS > B2K > UNCLASSIFIED with conflict flags |
| **Channel/geo tags** | ONLINE/OFFLINE/IN_APP, DOMESTIC/CROSS_BORDER |
| **Structural flags** | is_topup, is_wallet, is_processor, is_marketplace, is_mall, is_premium, is_ride_hailing, is_subscription, is_grocery, is_risky_merchant, is_mall_pattern, platform_ecosystem |
| **Behavioral tags** | Size tier, 6 granular time patterns (morning/late-morning/lunch/afternoon/evening/late-night), weekpart, day-of-week, payday window, business proxy, travel booking |
| **Recurrence engine** | CV-based (< 0.25 for subscriptions), amount stability, visit count |
| **Customer-month features** | 110+ features spanning foundation, composition, intelligence, risk, behavioral |
| **Merchant intelligence** | Entropy, HHI, top1/top3 share, scale, loyalty, novelty, turnover, churn, retention, repeat ratio |
| **Consumption metrics** | consumption_spend_m (ex-topup), txn_per_active_day_m, spend_vs_3m_avg_ratio |
| **L1 spend composition** | 12 ecosystem shares + dominant/secondary + category breadth + platform ecosystem (Grab/LINE/Shopee/True/Lazada/Robinhood/KBank) |
| **L2 key shares** | 21 key category shares for campaigns/pricing |
| **Recurrence features** | Subscription/recurring merchant counts, spend shares, interval stats |
| **Wallet/payment** | Wallet dependence, topup heavy, processor mediated flags |
| **Rolling windows** | 3M, 6M, and 12M for spend, ticket, share, entropy, stability |
| **Velocity/trends** | MoM spend/txn/ticket/share changes, acceleration, entropy trend |
| **Migration** | L1 migration flag, diversification flag, concentration shifts |
| **Regime/quality** | ACTIVE/DORMANT/REACTIVATING/LOW_VOLUME, quality checks |
| **Premium/affluence** | Retail orientation, affluence proxy, travel premium, value-seeking |
| **Risk monitoring** | Concentration shock, spend drop, topup overdependence, category shock |
| **Persona dimension scores** | 8 explicit dimension scores (travel, affluent, digital, food_social, everyday, business, mobile_wallet, concentration) |
| **Primary personas** | 10 mutually exclusive personas (rolling 3M based, deterministic precedence) |
| **Micro-segments** | 16 non-exclusive overlay flags (including grocery_anchor, payday_spender, ride_hailing_regular) |
| **Persona confidence** | HIGH/MEDIUM/LOW based on feature quality + numeric score (0–1) for ML/ranking |
| **Persona stability** | Shift flag, tenure months |
| **Use-case linkage** | Campaign, CLM/line, pricing, risk — per persona |

### Audit feedback incorporated
- **Kimi audit:** Separated wallet top-up from consumption spend; added defensive timestamp handling
  (midnight default detection); strengthened recurrence engine beyond simple 25-35 day interval;
  added taxonomy conflict detection between Mobius and B2K.
- **Gemini audit:** Made all window operations deterministic (row_number over priority, not F.first);
  added feature quality flags; added null-rate checks; added merchant turnover and loyalty features;
  marked approximate features clearly (business proxy, risk adjacent).
- **V4 production audit:** Fixed 5 runtime bugs (turnover select, QA column refs, Boots taxonomy,
  duplicate aggregation). Added consumption_spend_m, txn_per_active_day_m, merchant_repeat_ratio_m,
  merchant_novelty_rate_m, merchant_churn/retention_rate_m, spend_vs_3m_avg_ratio,
  numeric persona_confidence_score. Expanded campaign_view with new features.
- **Taxonomy coverage audit:** Added 12 incremental rules for healthcare gaps (generic hospital/clinic/
  diagnostic/dental/optical), personal care (salon/cosmetic clinic/nail), utilities (gas/ISP/bill pay),
  and pet/vet. Added platform_ecosystem column (Grab/LINE/Shopee/True/Lazada/Robinhood/KBank)
  with customer-month aggregation (dominant_platform, platform_count_m, platform_share_m).

### Intentionally deferred
1. **ML-based merchant classification** — requires labeled training data; deterministic rules serve as Phase 1.
2. **Salary/payday burstiness** — payday window proxy (DOM 25-5) now included; exact salary date inference still deferred.
3. **Late-night risk markers** — only available if timestamps are genuine (not midnight-padded).
4. **Installment detection** — requires installment plan metadata not available in base transaction table.
5. **Champion-challenger taxonomy A/B testing** — infrastructure concern, not notebook scope.
6. **500+ merchant rule expansion** — seed provided; production team must expand using unclassified audit.
7. **Persona persistence smoothing** — rolling 3M used as proxy; Markov-chain smoothing deferred.

### Assumptions that remain
- `CUST_NUM` is a stable customer identifier across months.
- `TXN_DT` provides at least date-level granularity; timestamp precision may vary.
- B2K and Mobius reference tables will be populated with real governed data.
- Merchant descriptors are issuer-provided (not network-cleaned) — normalization handles noise.
- Currency field (`CURR_DESC` or `TXN_CURR_CD`) reliably indicates domestic vs cross-border.
- Mega/large/longtail merchant thresholds (100K/10K/1K txns) will be calibrated on real data.
"""))

# ═══════════════════════════════════════════════════════════════════════════════
# BUILD NOTEBOOK
# ═══════════════════════════════════════════════════════════════════════════════
notebook = {
    "cells": cells,
    "metadata": {
        "kernelspec": {
            "display_name": "Python 3",
            "language": "python",
            "name": "python3"
        },
        "language_info": {
            "name": "python",
            "version": "3.11"
        }
    },
    "nbformat": 4,
    "nbformat_minor": 5
}

output_path = os.path.expanduser("~/Documents/Card_Spend_Intelligence_V4_Master_Notebook.ipynb")
with open(output_path, "w") as f:
    json.dump(notebook, f, indent=2)

print(f"Notebook generated: {output_path}")
print(f"Total cells: {len(cells)}")
print(f"  Markdown cells: {sum(1 for c in cells if c['cell_type'] == 'markdown')}")
print(f"  Code cells: {sum(1 for c in cells if c['cell_type'] == 'code')}")
