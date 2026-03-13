# CARD SPEND INTELLIGENCE MASTER PIPELINE
# ================================================================
# Production-style PySpark pipeline for:
# 1) Merchant normalization
# 2) Taxonomy reconciliation (Mobius / B2K / deterministic rules)
# 3) Transaction structural + behavioral tags
# 4) Customer-month feature mart
# 5) Rolling behavior vectors
# 6) Persona engine
# 7) Use-case output views
# ================================================================
# Notes
# - This file is designed as a strong production starter.
# - It avoids point-in-time leakage for customer-month features.
# - It supports optional source columns defensively.
# - It is intentionally verbose and auditable.
# ================================================================

from pyspark.sql import DataFrame, Window
from pyspark.sql import functions as F
from pyspark.sql import types as T
from functools import reduce
from datetime import datetime

# ================================================================
# 0. CONFIG
# ================================================================

SRC_TABLE = "cdx_mdz_prd.cdx_curated_crcard_acl_db.crcard_txn_dly"
PHASE1_OUTPUT_TABLE = "tmp.card_spend_txn_enriched_v1"
PHASE2_OUTPUT_TABLE = "tmp.card_spend_feature_mart_v1"
PHASE3_OUTPUT_TABLE = "tmp.card_spend_persona_mart_v1"
AUDIT_TABLE = "tmp.card_spend_pipeline_audit_v1"

TXN_DATE_COL = "TXN_DT"
TXN_TS_COL = "TXN_TS"          # optional
AMOUNT_COL = "TXN_AMT"
CUSTOMER_COL = "CUST_NUM"
MERCHANT_COL = "MCHT_NM"
TXN_DESC_COL = "TXN_DESC"
CURRENCY_COL = "CURR_DESC"
ONLINE_FLAG_COL = "ONLN_FLAG"

OPTIONAL_SOURCE_COLS = [
    "MCHT_CATG", "MCHT_SUB_CATG", "BROAD_CATEGORY", "SUB_CATEGORY",
    "taxonomy_l1_final", "taxonomy_l2_final", "taxonomy_l3_final",
    "SIC_CD_DESC", "MCC", "MCC_DESC"
]

TOPUP_REGEX = r"(top\s?up|wallet\s?top\s?up|stored\s?value|poi funding|money transfer|money orders)"
WALLET_REGEX = r"(truemoney|true\s?money|rabbit\s?line\s?pay|line\s?pay|linepay|shopee\s?pay|shopeepay|blueplus|grabpay)"
PROCESSOR_REGEX = r"(paypal|2c2p|omise|xendit|gbprimepay|paysolutions|siampay|lianlian|payw\*|kbank\*meta pay|meta pay)"
MARKETPLACE_REGEX = r"(shopee|lazada|amazon|ebay|aliexpress)"
FOOD_DELIVERY_REGEX = r"(grabfood|foodpanda|lineman|line\s?man|robinhood)"
RIDE_HAILING_REGEX = r"(grab|bolt)"

LUXURY_MALL_REGEX = r"(paragon|iconsiam|emporium|emquartier|central embassy|gaysorn|central chidlom|one bangkok)"
CENTRAL_REGEX = r"(central|central plaza|central festival|central pattana|central westgate|central ladprao|central rama)"
THE_MALL_REGEX = r"(the mall|ngamwongwan|bangkapi|thapra|bangkae)"
SIAM_EM_REGEX = r"(siam paragon|emporium|emquartier|emsphere|siam center|siam discovery)"
MEGA_REGEX = r"(mega bangna|fashion island|terminal 21|future park|seacon|westgate)"

FUEL_REGEX = r"(ptt|bangchak|shell|esso|caltex|susco|fuel|service station)"
TELCO_REGEX = r"(ais|truemove|true move|dtac|3bb|nt broadband|telecom|telephone|broadband)"
GROCERY_REGEX = r"(big c|lotus|tesco|makro|tops|gourmet market|foodland|villa market|maxvalu|maxvalue)"
CONVENIENCE_REGEX = r"(7\s?-?eleven|7/11|familymart|family mart|mini big c)"
PHARMACY_REGEX = r"(boots|watsons|fascino|save drug|drug stores|pharmacy)"
COFFEE_REGEX = r"(starbucks|cafe amazon|true coffee|punthai|coffee|cafe)"
QSR_REGEX = r"(mcdonald|kfc|burger king|subway|chester|swensen|pizza|sizzler|mk restaurant|yayoi)"
HOME_REGEX = r"(homepro|thaiwatsadu|global house|ikea|index living|furniture|hardware|building materials)"
BOOKS_REGEX = r"(naiin|asia books|b2s|se-ed|book|stationery|office supplies)"
SPORT_REGEX = r"(supersports|sport|soccer pro|warhammer|fitness|golf|bowling|pool)"
HEALTH_REGEX = r"(hospital|clinic|medical|dental|bumrungrad|bangkok hospital|samitivej)"
HOTEL_REGEX = r"(hotel|resort|marriott|hilton|hyatt|sheraton|ibis|novotel|holiday inn|movenpick|four seasons)"
AIRLINE_REGEX = r"(airways|airlines|airline|emirates|thai airways|lufthansa|qatar airways|singapore airlines|airasia|jetstar)"
OTA_REGEX = r"(agoda|booking\.com|expedia|trip\.com|traveloka)"
B2B_REGEX = r"(office|logistics|courier|professional services|consulting|legal|accounting|warehouse|industrial supplies|wholesale)"
INSURANCE_REGEX = r"(insurance|premiums|underwriting)"
INVEST_REGEX = r"(securities|broker|brokerage|trading|investment|etoro|plus500|binance|coinbase|okx)"

OUTPUT_REQUIRED_COLS = [
    CUSTOMER_COL,
    "txn_date",
    "txn_month",
    "txn_amount",
    "merchant_key",
    "mcht_nm_norm",
    "match_text",
    "tax_l1",
    "tax_l2",
    "tax_l3",
    "channel_tag",
    "geo_tag",
    "in_app_tag",
    "wallet_flag",
    "topup_flag",
    "processor_flag",
    "taxonomy_source_used",
    "taxonomy_confidence_tag"
]

# ================================================================
# 1. HELPERS
# ================================================================

def col_exists(df: DataFrame, c: str) -> bool:
    return c in df.columns


def safe_col(df: DataFrame, c: str, default=None):
    if c in df.columns:
        return F.col(c)
    return F.lit(default)


def norm_expr(c):
    return F.trim(
        F.regexp_replace(
            F.regexp_replace(F.lower(F.coalesce(c, F.lit(""))), r"[^a-z0-9ก-๙\s\*\-\./&]", " "),
            r"\s+",
            " "
        )
    )


def ensure_audit_table():
    spark.sql(f"""
    CREATE TABLE IF NOT EXISTS {AUDIT_TABLE} (
        phase STRING,
        processed_dtm TIMESTAMP,
        metric STRING,
        metric_value STRING
    ) USING DELTA
    """)


def append_audit(phase: str, metrics: dict):
    rows = [(phase, datetime.now(), k, str(v)) for k, v in metrics.items()]
    audit_df = spark.createDataFrame(rows, ["phase", "processed_dtm", "metric", "metric_value"])
    audit_df.write.mode("append").format("delta").saveAsTable(AUDIT_TABLE)


# ================================================================
# 2. SOURCE READ + BASIC STANDARDIZATION
# ================================================================

def read_transactions() -> DataFrame:
    df = spark.table(SRC_TABLE)

    txn_date = (
        F.to_date(F.col(TXN_DATE_COL))
        if col_exists(df, TXN_DATE_COL)
        else F.to_date(F.current_timestamp())
    )

    txn_amount = (
        F.col(AMOUNT_COL).cast("double")
        if col_exists(df, AMOUNT_COL)
        else F.lit(0.0)
    )

    out = (
        df
        .withColumn("txn_date", txn_date)
        .withColumn("txn_month", F.date_trunc("month", F.col("txn_date")).cast("date"))
        .withColumn("txn_amount", txn_amount)
        .withColumn("mcht_nm_raw", safe_col(df, MERCHANT_COL, ""))
        .withColumn("txn_desc_raw", safe_col(df, TXN_DESC_COL, ""))
        .withColumn("curr_desc_raw", safe_col(df, CURRENCY_COL, ""))
        .withColumn("onln_flag_raw", safe_col(df, ONLINE_FLAG_COL, ""))
        .withColumn("cust_num", safe_col(df, CUSTOMER_COL, None))
    )

    return out


# ================================================================
# 3. MERCHANT NORMALIZATION + TOKENIZATION
# ================================================================

def normalize_transactions(df: DataFrame) -> DataFrame:
    df1 = (
        df
        .withColumn("mcht_nm_norm", norm_expr(F.col("mcht_nm_raw")))
        .withColumn("txn_desc_norm", norm_expr(F.col("txn_desc_raw")))
        .withColumn("curr_desc_norm", norm_expr(F.col("curr_desc_raw")))
        .withColumn("match_text", F.concat_ws(" | ", F.col("mcht_nm_norm"), F.col("txn_desc_norm")))
        .withColumn("merchant_tokens", F.split(F.col("mcht_nm_norm"), " "))
        .withColumn("merchant_key",
            F.sha2(F.concat_ws("||", F.col("mcht_nm_norm"), F.coalesce(F.col("txn_desc_norm"), F.lit(""))), 256)
        )
    )
    return df1


# ================================================================
# 4. OPTIONAL SOURCE MAPPINGS (B2K / MOBIUS NORMALIZATION)
# ================================================================

def attach_source_taxonomy(df: DataFrame) -> DataFrame:
    # These are intentionally defensive; source columns may not exist.
    mobius_l1 = None
    mobius_l2 = None
    mobius_l3 = None
    b2k_l1 = None
    b2k_l2 = None
    b2k_l3 = None

    if col_exists(df, "taxonomy_l1_final"):
        # generic source landing, later split heuristically
        generic_l1 = F.when(~F.col("taxonomy_l1_final").isin("#N/A", "null", ""), F.col("taxonomy_l1_final"))
        generic_l2 = F.when(~F.col("taxonomy_l2_final").isin("#N/A", "null", ""), F.col("taxonomy_l2_final"))
        generic_l3 = F.when(~F.col("taxonomy_l3_final").isin("#N/A", "null", ""), F.col("taxonomy_l3_final"))

        # If SIC_CD_DESC exists, assume this resembles Mobius-style source.
        if col_exists(df, "SIC_CD_DESC"):
            mobius_l1, mobius_l2, mobius_l3 = generic_l1, generic_l2, generic_l3
        else:
            b2k_l1, b2k_l2, b2k_l3 = generic_l1, generic_l2, generic_l3

    df2 = df
    for name, expr in [
        ("mobius_l1", mobius_l1), ("mobius_l2", mobius_l2), ("mobius_l3", mobius_l3),
        ("b2k_l1", b2k_l1), ("b2k_l2", b2k_l2), ("b2k_l3", b2k_l3),
    ]:
        df2 = df2.withColumn(name, expr if expr is not None else F.lit(None).cast("string"))

    return df2


# ================================================================
# 5. DETERMINISTIC MERCHANT RULES
# ================================================================

def build_rule_df() -> DataFrame:
    rules = [
        # high-confidence tech / digital
        (10, "DIGITAL_SERVICES", "BIG_TECH", "GOOGLE_ECOSYSTEM", r"\b(google(\*| )?(play|services|ads)?|youtube)\b"),
        (10, "DIGITAL_SERVICES", "BIG_TECH", "META_ECOSYSTEM", r"\b(meta(\*| )?pay|facebook|instagram(\*| )?ads?)\b"),
        (10, "DIGITAL_SERVICES", "BIG_TECH", "MICROSOFT_ECOSYSTEM", r"\b(microsoft(\s?store)?|xbox)\b"),
        (10, "DIGITAL_SERVICES", "STREAMING", "STREAMING_CONTENT", r"\b(netflix|spotify|disney|youtube premium|apple music)\b"),
        (10, "DIGITAL_SERVICES", "GAMING", "GAMING_PLATFORMS", r"\b(steam|roblox|garena|mlbb|playstation)\b"),

        # wallet / processors / marketplace
        (15, "FINANCIAL_SERVICES", "DIGITAL_WALLET", "DIGITAL_WALLET_TOPUP", WALLET_REGEX),
        (15, "FINANCIAL_SERVICES", "PAYMENT_INFRA", "PAYMENT_PROCESSORS", PROCESSOR_REGEX),
        (20, "SHOPPING_RETAIL", "MARKETPLACE_ECOM", "ECOM_MARKETPLACE_TH", MARKETPLACE_REGEX),

        # food / dining / mobility
        (25, "FOOD_DINING", "FOOD_DELIVERY", "FOOD_DELIVERY_APP", FOOD_DELIVERY_REGEX),
        (25, "TRAVEL_MOBILITY", "RIDE_HAILING", "RIDE_HAILING", RIDE_HAILING_REGEX),
        (30, "FOOD_DINING", "COFFEE_CAFE", "COFFEE_CHAINS", COFFEE_REGEX),
        (30, "FOOD_DINING", "FAST_FOOD_QSR", "RESTAURANT_QSR", QSR_REGEX),

        # essentials
        (40, "EVERYDAY_ESSENTIALS", "GROCERY_MODERN", "RETAIL_GROCERY_MODERN", GROCERY_REGEX),
        (40, "EVERYDAY_ESSENTIALS", "CONVENIENCE_STORE", "CONVENIENCE_STORE", CONVENIENCE_REGEX),
        (40, "EVERYDAY_ESSENTIALS", "PHARMACY_DRUGSTORE", "PHARMACY_CHAIN", PHARMACY_REGEX),
        (40, "AUTOMOTIVE", "FUEL", "FUEL_STATION", FUEL_REGEX),
        (40, "EVERYDAY_ESSENTIALS", "TELCO_MOBILE", "TELCO_MOBILE", TELCO_REGEX),

        # travel
        (50, "TRAVEL_MOBILITY", "HOTELS", "HOTEL", HOTEL_REGEX),
        (50, "TRAVEL_MOBILITY", "AIRLINES", "AIRLINES", AIRLINE_REGEX),
        (50, "TRAVEL_MOBILITY", "OTA_TRAVEL", "OTA_TRAVEL", OTA_REGEX),

        # malls / premium / retail
        (60, "SHOPPING_RETAIL", "MALLS_DEPARTMENT", "SIAM_EM_DISTRICT", SIAM_EM_REGEX),
        (60, "SHOPPING_RETAIL", "MALLS_DEPARTMENT", "CENTRAL_GROUP_MALLS", CENTRAL_REGEX),
        (60, "SHOPPING_RETAIL", "MALLS_DEPARTMENT", "THE_MALL_GROUP", THE_MALL_REGEX),
        (60, "SHOPPING_RETAIL", "MALLS_DEPARTMENT", "MEGA_MALLS", MEGA_REGEX),
        (65, "SHOPPING_RETAIL", "PREMIUM_RETAIL", "LUXURY_FASHION_BRANDS", LUXURY_MALL_REGEX),
        (70, "SHOPPING_RETAIL", "BOOKS_STATIONERY", "BOOKS_STATIONERY", BOOKS_REGEX),
        (70, "ENTERTAINMENT_LEISURE", "SPORTS_ACTIVITY", "SPECIALITY_SPORT", SPORT_REGEX),

        # health / home / finance / business
        (80, "HEALTH_WELLNESS", "HOSPITAL_CLINIC", "HOSPITAL_MEDICAL", HEALTH_REGEX),
        (80, "HOME_LIVING", "HOME_IMPROVEMENT", "HOMEMATERIAL_FURNITURE", HOME_REGEX),
        (85, "FINANCIAL_SERVICES", "INSURANCE", "INSURANCE_PAYMENTS", INSURANCE_REGEX),
        (85, "FINANCIAL_SERVICES", "INVESTMENT_TRADING", "TRADING_PLATFORMS", INVEST_REGEX),
        (90, "B2B_PROFESSIONAL", "PROFESSIONAL_SERVICES", "B2B_PROFESSIONAL", B2B_REGEX),
    ]

    schema = T.StructType([
        T.StructField("priority", T.IntegerType(), False),
        T.StructField("rule_l1", T.StringType(), False),
        T.StructField("rule_l2", T.StringType(), False),
        T.StructField("rule_l3", T.StringType(), False),
        T.StructField("regex_pattern", T.StringType(), False),
    ])
    return spark.createDataFrame(rules, schema=schema)


def apply_rules(df: DataFrame, rule_df: DataFrame) -> DataFrame:
    matched = (
        df.alias("t")
        .join(F.broadcast(rule_df).alias("r"), F.expr("t.match_text rlike r.regex_pattern"), "left")
        .withColumn("rule_matched", F.when(F.col("r.priority").isNotNull(), F.lit(1)).otherwise(F.lit(0)))
    )

    group_cols = [c for c in df.columns]

    agg = (
        matched
        .groupBy(*group_cols)
        .agg(
            F.min("r.priority").alias("matched_priority"),
            F.collect_set(F.when(F.col("r.priority").isNotNull(), F.concat_ws("|", "r.rule_l1", "r.rule_l2", "r.rule_l3"))).alias("matched_rule_set"),
            F.sum("rule_matched").alias("rule_match_count")
        )
    )

    best = (
        agg.alias("a")
        .join(
            F.broadcast(rule_df).alias("r"),
            (F.col("a.matched_priority") == F.col("r.priority")) &
            (F.size(F.col("a.matched_rule_set")) > 0),
            "left"
        )
        .withColumn("rule_l1", F.col("r.rule_l1"))
        .withColumn("rule_l2", F.col("r.rule_l2"))
        .withColumn("rule_l3", F.col("r.rule_l3"))
        .drop("priority", "regex_pattern")
        .withColumn("rule_conflict_flag",
            F.when(F.col("rule_match_count") > 1, F.lit("MULTIPLE_RULES_MATCHED")).when(F.col("rule_match_count") == 1, F.lit("SINGLE_RULE_MATCHED")).otherwise(F.lit("NO_RULE_MATCH"))
        )
    )

    return best


# ================================================================
# 6. STRUCTURAL TAGS
# ================================================================

def add_structural_tags(df: DataFrame) -> DataFrame:
    channel_tag = (
        F.when(F.col("match_text").rlike(FOOD_DELIVERY_REGEX) | F.col("match_text").rlike(RIDE_HAILING_REGEX) | F.col("match_text").rlike(WALLET_REGEX), F.lit("IN_APP"))
         .when(F.col("onln_flag_raw").isin("Y", "1"), F.lit("ONLINE"))
         .when(F.col("onln_flag_raw").isin("N", "0"), F.lit("OFFLINE"))
         .otherwise(F.lit("UNKNOWN"))
    )

    geo_tag = (
        F.when(F.col("curr_desc_norm").rlike(r"\b(thb|baht)\b"), F.lit("DOMESTIC"))
         .when(F.col("curr_desc_norm") == "", F.lit("UNKNOWN"))
         .otherwise(F.lit("CROSS_BORDER"))
    )

    return (
        df
        .withColumn("channel_tag", channel_tag)
        .withColumn("geo_tag", geo_tag)
        .withColumn("in_app_tag", F.when(F.col("channel_tag") == "IN_APP", F.lit(1)).otherwise(F.lit(0)))
        .withColumn("wallet_flag", F.when(F.col("match_text").rlike(WALLET_REGEX), F.lit(1)).otherwise(F.lit(0)))
        .withColumn("processor_flag", F.when(F.col("match_text").rlike(PROCESSOR_REGEX), F.lit(1)).otherwise(F.lit(0)))
        .withColumn("topup_flag",
            F.when(F.col("match_text").rlike(TOPUP_REGEX), F.lit(1))
             .when(F.col("wallet_flag") == 1, F.lit(1))
             .otherwise(F.lit(0))
        )
        .withColumn("marketplace_flag", F.when(F.col("match_text").rlike(MARKETPLACE_REGEX), F.lit(1)).otherwise(F.lit(0)))
        .withColumn("food_delivery_flag", F.when(F.col("match_text").rlike(FOOD_DELIVERY_REGEX), F.lit(1)).otherwise(F.lit(0)))
        .withColumn("ride_hailing_flag", F.when(F.col("match_text").rlike(RIDE_HAILING_REGEX), F.lit(1)).otherwise(F.lit(0)))
        .withColumn("premium_merchant_flag", F.when(F.col("match_text").rlike(LUXURY_MALL_REGEX), F.lit(1)).otherwise(F.lit(0)))
        .withColumn("business_proxy_flag", F.when(F.col("match_text").rlike(B2B_REGEX), F.lit(1)).otherwise(F.lit(0)))
        .withColumn("mall_ecosystem_flag", F.when(F.col("match_text").rlike(f"{CENTRAL_REGEX}|{THE_MALL_REGEX}|{SIAM_EM_REGEX}|{MEGA_REGEX}"), F.lit(1)).otherwise(F.lit(0)))
    )


# ================================================================
# 7. TAXONOMY RECONCILIATION
# ================================================================

def valid_tax_col(c):
    return (~F.col(c).isin("#N/A", "null", "", "UNCLASSIFIED")) & F.col(c).isNotNull()


def reconcile_taxonomy(df: DataFrame) -> DataFrame:
    # High-confidence rules can override weak generic sources.
    df1 = (
        df
        .withColumn(
            "taxonomy_source_used",
            F.when(F.col("matched_priority").isNotNull(), F.lit("MERCHANT_RULE"))
             .when(valid_tax_col("mobius_l1"), F.lit("MOBIUS"))
             .when(valid_tax_col("b2k_l1"), F.lit("B2K"))
             .otherwise(F.lit("UNCLASSIFIED"))
        )
        .withColumn(
            "taxonomy_conflict_flag",
            F.when(valid_tax_col("mobius_l1") & valid_tax_col("b2k_l1") & (F.col("mobius_l1") != F.col("b2k_l1")), F.lit("MOBIUS_B2K_MISMATCH"))
             .otherwise(F.lit("NO_CONFLICT"))
        )
        .withColumn(
            "tax_l1",
            F.when(F.col("taxonomy_source_used") == "MERCHANT_RULE", F.col("rule_l1"))
             .when(F.col("taxonomy_source_used") == "MOBIUS", F.col("mobius_l1"))
             .when(F.col("taxonomy_source_used") == "B2K", F.col("b2k_l1"))
             .otherwise(F.lit("OTHER_LONGTAIL"))
        )
        .withColumn(
            "tax_l2",
            F.when(F.col("taxonomy_source_used") == "MERCHANT_RULE", F.col("rule_l2"))
             .when(F.col("taxonomy_source_used") == "MOBIUS", F.col("mobius_l2"))
             .when(F.col("taxonomy_source_used") == "B2K", F.col("b2k_l2"))
             .otherwise(F.lit("UNCLASSIFIED"))
        )
        .withColumn(
            "tax_l3",
            F.when(F.col("taxonomy_source_used") == "MERCHANT_RULE", F.col("rule_l3"))
             .when(F.col("taxonomy_source_used") == "MOBIUS", F.col("mobius_l3"))
             .when(F.col("taxonomy_source_used") == "B2K", F.col("b2k_l3"))
             .otherwise(F.lit("UNCLASSIFIED"))
        )
    )

    df2 = (
        df1
        .withColumn(
            "taxonomy_confidence_tag",
            F.when(F.col("taxonomy_source_used") == "MERCHANT_RULE", F.lit("HIGH"))
             .when((F.col("taxonomy_source_used").isin("MOBIUS", "B2K")) & (F.col("tax_l3") != "UNCLASSIFIED"), F.lit("MEDIUM"))
             .when((F.col("taxonomy_source_used").isin("MOBIUS", "B2K")) & (F.col("tax_l2") != "UNCLASSIFIED"), F.lit("LOW"))
             .otherwise(F.lit("VERY_LOW"))
        )
        .withColumn(
            "taxonomy_match_level",
            F.when(F.col("tax_l3") != "UNCLASSIFIED", F.lit("L3"))
             .when(F.col("tax_l2") != "UNCLASSIFIED", F.lit("L2"))
             .when(F.col("tax_l1") != "OTHER_LONGTAIL", F.lit("L1"))
             .otherwise(F.lit("NONE"))
        )
    )
    return df2


# ================================================================
# 8. TRANSACTION BEHAVIOR TAGS
# ================================================================

def add_txn_behavior_tags(df: DataFrame) -> DataFrame:
    size_tier = (
        F.when(F.col("txn_amount") < 100, "MICRO")
         .when(F.col("txn_amount") < 500, "SMALL")
         .when(F.col("txn_amount") < 2000, "STANDARD")
         .when(F.col("txn_amount") < 10000, "MID")
         .when(F.col("txn_amount") < 50000, "LARGE")
         .otherwise("PREMIUM")
    )

    w_cust_merchant = Window.partitionBy("cust_num", "merchant_key").orderBy("txn_date")

    df1 = (
        df
        .withColumn("txn_size_tier", size_tier)
        .withColumn("prev_txn_dt_same_merchant", F.lag("txn_date").over(w_cust_merchant))
        .withColumn("prev_txn_amt_same_merchant", F.lag("txn_amount").over(w_cust_merchant))
        .withColumn("days_since_prev_same_merchant",
            F.when(F.col("prev_txn_dt_same_merchant").isNotNull(), F.datediff(F.col("txn_date"), F.col("prev_txn_dt_same_merchant")))
        )
        .withColumn("amt_delta_prev_same_merchant",
            F.when(
                F.col("prev_txn_amt_same_merchant").isNotNull() & (F.col("prev_txn_amt_same_merchant") != 0),
                F.abs(F.col("txn_amount") - F.col("prev_txn_amt_same_merchant")) / F.abs(F.col("prev_txn_amt_same_merchant"))
            )
        )
    )

    w_3 = Window.partitionBy("cust_num", "merchant_key").orderBy("txn_date").rowsBetween(-2, 0)

    df2 = (
        df1
        .withColumn("avg_interval_3txns", F.avg("days_since_prev_same_merchant").over(w_3))
        .withColumn("std_interval_3txns", F.stddev_samp("days_since_prev_same_merchant").over(w_3))
        .withColumn("cv_interval",
            F.when(F.col("avg_interval_3txns") > 0, F.col("std_interval_3txns") / F.col("avg_interval_3txns"))
        )
        .withColumn("merchant_novelty_tag",
            F.when(F.col("prev_txn_dt_same_merchant").isNull(), F.lit("FIRST_TIME")).otherwise(F.lit("REPEAT"))
        )
        .withColumn(
            "recurrence_tag",
            F.when(F.col("prev_txn_dt_same_merchant").isNull(), F.lit("FIRST_TIME"))
             .when(
                 F.col("days_since_prev_same_merchant").between(20, 45) &
                 (F.coalesce(F.col("cv_interval"), F.lit(999.0)) < 0.35) &
                 (F.coalesce(F.col("amt_delta_prev_same_merchant"), F.lit(999.0)) < 0.20),
                 F.lit("SUBSCRIPTION_LIKE")
             )
             .when(
                 F.col("days_since_prev_same_merchant").between(7, 60) &
                 (F.coalesce(F.col("cv_interval"), F.lit(999.0)) < 0.60),
                 F.lit("RECURRING")
             )
             .when(F.col("days_since_prev_same_merchant").between(1, 120), F.lit("IRREGULAR"))
             .otherwise(F.lit("EPISODIC"))
        )
        .withColumn("subscription_candidate_flag",
            F.when(F.col("recurrence_tag").isin("SUBSCRIPTION_LIKE", "RECURRING"), F.lit(1)).otherwise(F.lit(0))
        )
    )
    return df2


# ================================================================
# 9. PHASE 1 PIPELINE
# ================================================================

def run_phase1() -> DataFrame:
    ensure_audit_table()

    txn0 = read_transactions()
    txn1 = normalize_transactions(txn0)
    txn2 = attach_source_taxonomy(txn1)
    rule_df = build_rule_df()
    txn3 = apply_rules(txn2, rule_df)
    txn4 = add_structural_tags(txn3)
    txn5 = reconcile_taxonomy(txn4)
    txn6 = add_txn_behavior_tags(txn5)

    for c in OUTPUT_REQUIRED_COLS:
        if c not in txn6.columns:
            txn6 = txn6.withColumn(c, F.lit(None).cast("string"))

    txn_enriched = (
        txn6
        .withColumn("_processed_dtm", F.current_timestamp())
        .withColumn("_phase", F.lit("PHASE1"))
        .withColumn("_src_table", F.lit(SRC_TABLE))
    )

    txn_enriched.write.mode("overwrite").format("delta").saveAsTable(PHASE1_OUTPUT_TABLE)

    total = txn_enriched.count()
    unclassified = txn_enriched.filter(F.col("tax_l1") == "OTHER_LONGTAIL").count()
    rule_cov = txn_enriched.filter(F.col("taxonomy_source_used") == "MERCHANT_RULE").count()
    mobius_cov = txn_enriched.filter(F.col("taxonomy_source_used") == "MOBIUS").count()
    b2k_cov = txn_enriched.filter(F.col("taxonomy_source_used") == "B2K").count()

    append_audit("PHASE1", {
        "output_table": PHASE1_OUTPUT_TABLE,
        "total_records": total,
        "unclassified_pct": round(100.0 * unclassified / total, 4) if total else 0.0,
        "rule_coverage_pct": round(100.0 * rule_cov / total, 4) if total else 0.0,
        "mobius_coverage_pct": round(100.0 * mobius_cov / total, 4) if total else 0.0,
        "b2k_coverage_pct": round(100.0 * b2k_cov / total, 4) if total else 0.0,
    })

    return txn_enriched


# ================================================================
# 10. FEATURE MART HELPERS
# ================================================================

def share_expr(cond_col: str):
    return F.sum(F.when(F.col(cond_col), F.col("txn_amount")).otherwise(F.lit(0.0)))


def cnt_expr(cond_col: str):
    return F.sum(F.when(F.col(cond_col), F.lit(1)).otherwise(F.lit(0)))


def safe_div(num, den):
    return F.when(den.isNull() | (den == 0), F.lit(0.0)).otherwise(num / den)


# ================================================================
# 11. CUSTOMER-MONTH BASE AGGREGATION
# ================================================================

def build_customer_month_base(txn: DataFrame) -> DataFrame:
    base = (
        txn
        .groupBy("cust_num", "txn_month")
        .agg(
            F.count("*").alias("txn_cnt_m"),
            F.sum("txn_amount").alias("spend_amt_m"),
            F.avg("txn_amount").alias("avg_ticket_m"),
            F.max("txn_amount").alias("max_ticket_m"),
            F.countDistinct("txn_date").alias("active_days_m"),
            F.countDistinct("merchant_key").alias("uniq_merchants_m"),
            F.countDistinct("tax_l1").alias("uniq_tax_l1_m"),
            F.countDistinct("tax_l2").alias("uniq_tax_l2_m"),
            F.countDistinct("tax_l3").alias("uniq_tax_l3_m"),

            F.sum(F.when(F.col("channel_tag") == "ONLINE", F.col("txn_amount")).otherwise(F.lit(0.0))).alias("online_spend_m"),
            F.sum(F.when(F.col("channel_tag") == "OFFLINE", F.col("txn_amount")).otherwise(F.lit(0.0))).alias("offline_spend_m"),
            F.sum(F.when(F.col("channel_tag") == "IN_APP", F.col("txn_amount")).otherwise(F.lit(0.0))).alias("in_app_spend_m"),
            F.sum(F.when(F.col("geo_tag") == "DOMESTIC", F.col("txn_amount")).otherwise(F.lit(0.0))).alias("domestic_spend_m"),
            F.sum(F.when(F.col("geo_tag") == "CROSS_BORDER", F.col("txn_amount")).otherwise(F.lit(0.0))).alias("xborder_spend_m"),

            F.sum(F.when(F.col("wallet_flag") == 1, F.col("txn_amount")).otherwise(F.lit(0.0))).alias("wallet_spend_m"),
            F.sum(F.when(F.col("topup_flag") == 1, F.col("txn_amount")).otherwise(F.lit(0.0))).alias("topup_spend_m"),
            F.sum(F.when(F.col("processor_flag") == 1, F.col("txn_amount")).otherwise(F.lit(0.0))).alias("processor_spend_m"),
            F.sum(F.when(F.col("food_delivery_flag") == 1, F.col("txn_amount")).otherwise(F.lit(0.0))).alias("food_delivery_spend_m"),
            F.sum(F.when(F.col("ride_hailing_flag") == 1, F.col("txn_amount")).otherwise(F.lit(0.0))).alias("ride_hailing_spend_m"),
            F.sum(F.when(F.col("premium_merchant_flag") == 1, F.col("txn_amount")).otherwise(F.lit(0.0))).alias("premium_spend_m"),
            F.sum(F.when(F.col("business_proxy_flag") == 1, F.col("txn_amount")).otherwise(F.lit(0.0))).alias("business_proxy_spend_m"),
            F.sum(F.when(F.col("mall_ecosystem_flag") == 1, F.col("txn_amount")).otherwise(F.lit(0.0))).alias("mall_spend_m"),

            F.sum(F.when(F.col("subscription_candidate_flag") == 1, F.col("txn_amount")).otherwise(F.lit(0.0))).alias("recurring_spend_m"),
            F.sum(F.when(F.col("subscription_candidate_flag") == 1, F.lit(1)).otherwise(F.lit(0))).alias("recurring_txn_cnt_m"),
            F.countDistinct(F.when(F.col("subscription_candidate_flag") == 1, F.col("merchant_key"))).alias("recurring_merchant_cnt_m"),
        )
    )

    # L1 spend shares
    l1 = (
        txn.groupBy("cust_num", "txn_month", "tax_l1")
        .agg(F.sum("txn_amount").alias("l1_spend_m"))
    )

    l1_pivot = (
        l1.groupBy("cust_num", "txn_month")
        .pivot("tax_l1")
        .agg(F.first("l1_spend_m"))
    )

    # Dominant ecosystem
    w = Window.partitionBy("cust_num", "txn_month").orderBy(F.col("l1_spend_m").desc(), F.col("tax_l1"))
    dom = (
        l1.withColumn("rn", F.row_number().over(w))
          .filter(F.col("rn") == 1)
          .select("cust_num", "txn_month", F.col("tax_l1").alias("dominant_ecosystem"), F.col("l1_spend_m").alias("dominant_ecosystem_spend_m"))
    )

    out = base.join(l1_pivot, ["cust_num", "txn_month"], "left").join(dom, ["cust_num", "txn_month"], "left")

    # standard shares
    out = (
        out
        .withColumn("online_spend_share_m", safe_div(F.col("online_spend_m"), F.col("spend_amt_m")))
        .withColumn("offline_spend_share_m", safe_div(F.col("offline_spend_m"), F.col("spend_amt_m")))
        .withColumn("in_app_spend_share_m", safe_div(F.col("in_app_spend_m"), F.col("spend_amt_m")))
        .withColumn("xborder_spend_share_m", safe_div(F.col("xborder_spend_m"), F.col("spend_amt_m")))
        .withColumn("wallet_spend_share_m", safe_div(F.col("wallet_spend_m"), F.col("spend_amt_m")))
        .withColumn("topup_spend_share_m", safe_div(F.col("topup_spend_m"), F.col("spend_amt_m")))
        .withColumn("food_delivery_spend_share_m", safe_div(F.col("food_delivery_spend_m"), F.col("spend_amt_m")))
        .withColumn("premium_spend_share_m", safe_div(F.col("premium_spend_m"), F.col("spend_amt_m")))
        .withColumn("business_proxy_spend_share_m", safe_div(F.col("business_proxy_spend_m"), F.col("spend_amt_m")))
        .withColumn("mall_spend_share_m", safe_div(F.col("mall_spend_m"), F.col("spend_amt_m")))
        .withColumn("recurring_spend_share_m", safe_div(F.col("recurring_spend_m"), F.col("spend_amt_m")))
    )

    return out


# ================================================================
# 12. MERCHANT INTELLIGENCE FEATURES
# ================================================================

def build_merchant_intelligence(txn: DataFrame) -> DataFrame:
    # Merchant-month point-in-time scale
    merchant_month_stats = (
        txn.groupBy("merchant_key", "txn_month")
        .agg(
            F.count("*").alias("merchant_txn_cnt_m"),
            F.approx_count_distinct("cust_num").alias("merchant_cust_cnt_m"),
            F.sum("txn_amount").alias("merchant_total_spend_m")
        )
    )

    cust_merchant = (
        txn.select("cust_num", "txn_month", "merchant_key", "txn_amount")
           .join(merchant_month_stats, ["merchant_key", "txn_month"], "left")
           .groupBy("cust_num", "txn_month")
           .agg(
               F.avg("merchant_txn_cnt_m").alias("avg_merchant_scale_txn_cnt_m"),
               F.max("merchant_txn_cnt_m").alias("max_merchant_scale_txn_cnt_m"),
               F.sum(F.when(F.col("merchant_txn_cnt_m") >= 100000, F.col("txn_amount")).otherwise(F.lit(0.0))).alias("spend_at_mega_merchants_m"),
               F.sum(F.when((F.col("merchant_txn_cnt_m") >= 10000) & (F.col("merchant_txn_cnt_m") < 100000), F.col("txn_amount")).otherwise(F.lit(0.0))).alias("spend_at_large_merchants_m"),
               F.sum(F.when(F.col("merchant_txn_cnt_m") < 1000, F.col("txn_amount")).otherwise(F.lit(0.0))).alias("spend_at_longtail_merchants_m")
           )
    )

    # merchant entropy / hhi
    mshare = (
        txn.groupBy("cust_num", "txn_month", "merchant_key")
           .agg(F.sum("txn_amount").alias("merchant_spend_m"))
    )

    total = mshare.groupBy("cust_num", "txn_month").agg(F.sum("merchant_spend_m").alias("merchant_total_m"))
    mshare2 = mshare.join(total, ["cust_num", "txn_month"], "left").withColumn("merchant_share", safe_div(F.col("merchant_spend_m"), F.col("merchant_total_m")))

    mentropy = (
        mshare2.groupBy("cust_num", "txn_month")
            .agg(
                F.sum(F.when(F.col("merchant_share") > 0, -F.col("merchant_share") * F.log2(F.col("merchant_share"))).otherwise(F.lit(0.0))).alias("merchant_entropy_m"),
                F.sum(F.pow(F.col("merchant_share"), 2)).alias("merchant_hhi_m"),
                F.max("merchant_share").alias("top_merchant_share_m")
            )
    )

    top3 = (
        mshare2.withColumn("rn", F.row_number().over(Window.partitionBy("cust_num", "txn_month").orderBy(F.col("merchant_share").desc())))
               .filter(F.col("rn") <= 3)
               .groupBy("cust_num", "txn_month")
               .agg(F.sum("merchant_share").alias("top3_merchant_share_m"))
    )

    # turnover / new merchant count
    w_m = Window.partitionBy("cust_num", "merchant_key").orderBy("txn_month")
    merchant_presence = txn.select("cust_num", "txn_month", "merchant_key").dropDuplicates()
    merchant_presence = merchant_presence.withColumn("prev_seen_month", F.lag("txn_month").over(w_m))

    new_lost = (
        merchant_presence.groupBy("cust_num", "txn_month")
            .agg(
                F.sum(F.when(F.col("prev_seen_month").isNull(), 1).otherwise(0)).alias("new_merchant_cnt_m")
            )
    )

    return cust_merchant.join(mentropy, ["cust_num", "txn_month"], "left").join(top3, ["cust_num", "txn_month"], "left").join(new_lost, ["cust_num", "txn_month"], "left")


# ================================================================
# 13. ECOSYSTEM ENTROPY / HHI
# ================================================================

def build_ecosystem_concentration(txn: DataFrame) -> DataFrame:
    l1 = txn.groupBy("cust_num", "txn_month", "tax_l1").agg(F.sum("txn_amount").alias("l1_spend_m"))
    total = l1.groupBy("cust_num", "txn_month").agg(F.sum("l1_spend_m").alias("total_l1_spend_m"))
    l12 = l1.join(total, ["cust_num", "txn_month"], "left").withColumn("l1_share", safe_div(F.col("l1_spend_m"), F.col("total_l1_spend_m")))

    out = (
        l12.groupBy("cust_num", "txn_month")
           .agg(
               F.sum(F.when(F.col("l1_share") > 0, -F.col("l1_share") * F.log2(F.col("l1_share"))).otherwise(F.lit(0.0))).alias("ecosystem_entropy_m"),
               F.sum(F.pow(F.col("l1_share"), 2)).alias("ecosystem_hhi_m")
           )
    )
    return out


# ================================================================
# 14. ROLLING WINDOWS + VELOCITY
# ================================================================

def add_rolling_features(df: DataFrame) -> DataFrame:
    w_ord = Window.partitionBy("cust_num").orderBy("txn_month")
    w_3 = w_ord.rowsBetween(-2, 0)
    w_6 = w_ord.rowsBetween(-5, 0)

    out = (
        df
        .withColumn("spend_amt_3m", F.sum("spend_amt_m").over(w_3))
        .withColumn("txn_cnt_3m", F.sum("txn_cnt_m").over(w_3))
        .withColumn("spend_amt_6m", F.sum("spend_amt_m").over(w_6))
        .withColumn("avg_ticket_3m", F.avg("avg_ticket_m").over(w_3))
        .withColumn("merchant_entropy_3m_avg", F.avg("merchant_entropy_m").over(w_3))
        .withColumn("ecosystem_entropy_3m_avg", F.avg("ecosystem_entropy_m").over(w_3))
        .withColumn("prev_spend_amt_m", F.lag("spend_amt_m").over(w_ord))
        .withColumn("prev_txn_cnt_m", F.lag("txn_cnt_m").over(w_ord))
        .withColumn("prev_online_share_m", F.lag("online_spend_share_m").over(w_ord))
        .withColumn("prev_xborder_share_m", F.lag("xborder_spend_share_m").over(w_ord))
        .withColumn("prev_wallet_share_m", F.lag("wallet_spend_share_m").over(w_ord))
        .withColumn("prev_premium_share_m", F.lag("premium_spend_share_m").over(w_ord))
        .withColumn("prev_dominant_ecosystem", F.lag("dominant_ecosystem").over(w_ord))
        .withColumn("prev_merchant_entropy_m", F.lag("merchant_entropy_m").over(w_ord))
        .withColumn("mom_spend_growth", safe_div(F.col("spend_amt_m") - F.col("prev_spend_amt_m"), F.col("prev_spend_amt_m")))
        .withColumn("mom_txn_growth", safe_div(F.col("txn_cnt_m") - F.col("prev_txn_cnt_m"), F.col("prev_txn_cnt_m")))
        .withColumn("online_share_delta_m", F.col("online_spend_share_m") - F.coalesce(F.col("prev_online_share_m"), F.lit(0.0)))
        .withColumn("xborder_share_delta_m", F.col("xborder_spend_share_m") - F.coalesce(F.col("prev_xborder_share_m"), F.lit(0.0)))
        .withColumn("wallet_share_delta_m", F.col("wallet_spend_share_m") - F.coalesce(F.col("prev_wallet_share_m"), F.lit(0.0)))
        .withColumn("premium_share_delta_m", F.col("premium_spend_share_m") - F.coalesce(F.col("prev_premium_share_m"), F.lit(0.0)))
        .withColumn("l1_migration_flag", F.when((F.col("dominant_ecosystem") != F.col("prev_dominant_ecosystem")) & F.col("prev_dominant_ecosystem").isNotNull(), 1).otherwise(0))
        .withColumn("merchant_entropy_trend", F.col("merchant_entropy_m") - F.coalesce(F.col("prev_merchant_entropy_m"), F.lit(0.0)))
        .withColumn("diversification_flag",
            F.when(F.col("merchant_entropy_trend") > 0.10, F.lit("DIVERSIFYING"))
             .when(F.col("merchant_entropy_trend") < -0.10, F.lit("CONCENTRATING"))
             .otherwise(F.lit("STABLE"))
        )
    )
    return out


# ================================================================
# 15. SPEND REGIME + QUALITY FLAGS
# ================================================================

def add_regime_quality_flags(df: DataFrame) -> DataFrame:
    w_ord = Window.partitionBy("cust_num").orderBy("txn_month")

    out = (
        df
        .withColumn("prev_txn_cnt_m2", F.lag("txn_cnt_m", 2).over(w_ord))
        .withColumn(
            "spend_regime_flag",
            F.when(F.col("txn_cnt_m") >= 3, F.lit("ACTIVE"))
             .when((F.col("txn_cnt_m") > 0) & (F.coalesce(F.col("prev_txn_cnt_m"), F.lit(0)) == 0), F.lit("REACTIVATING"))
             .when(F.col("txn_cnt_m") == 0, F.lit("DORMANT"))
             .otherwise(F.lit("LOW_VOLUME"))
        )
        .withColumn(
            "wallet_dependence_flag",
            F.when(F.col("wallet_spend_share_m") > 0.50, F.lit("HIGH_WALLET"))
             .when(F.col("wallet_spend_share_m") > 0.20, F.lit("MODERATE_WALLET"))
             .otherwise(F.lit("LOW_WALLET"))
        )
        .withColumn(
            "feature_quality_flag",
            F.when(F.col("txn_cnt_m") < 3, F.lit("LOW_VOLUME"))
             .when(F.col("spend_amt_m") <= 0, F.lit("ZERO_SPEND"))
             .when(F.col("uniq_merchants_m") <= 1, F.lit("SINGLE_MERCHANT"))
             .otherwise(F.lit("GOOD"))
        )
    )
    return out


# ================================================================
# 16. PHASE 2 PIPELINE
# ================================================================

def run_phase2(txn_enriched: DataFrame) -> DataFrame:
    base = build_customer_month_base(txn_enriched)
    mintel = build_merchant_intelligence(txn_enriched)
    econ = build_ecosystem_concentration(txn_enriched)

    feat = base.join(mintel, ["cust_num", "txn_month"], "left").join(econ, ["cust_num", "txn_month"], "left")
    feat = add_rolling_features(feat)
    feat = add_regime_quality_flags(feat)

    feat = (
        feat
        .withColumn("_processed_dtm", F.current_timestamp())
        .withColumn("_phase", F.lit("PHASE2"))
    )

    feat.write.mode("overwrite").format("delta").saveAsTable(PHASE2_OUTPUT_TABLE)

    total = feat.count()
    low_volume = feat.filter(F.col("feature_quality_flag") == "LOW_VOLUME").count()
    append_audit("PHASE2", {
        "output_table": PHASE2_OUTPUT_TABLE,
        "customer_months": total,
        "low_volume_pct": round(100.0 * low_volume / total, 4) if total else 0.0,
    })

    return feat


# ================================================================
# 17. PERSONA ENGINE
# ================================================================

def add_missing_spend_cols(df: DataFrame) -> DataFrame:
    required = [
        "EVERYDAY_ESSENTIALS", "FOOD_DINING", "TRAVEL_MOBILITY", "SHOPPING_RETAIL",
        "HEALTH_WELLNESS", "DIGITAL_SERVICES", "ENTERTAINMENT_LEISURE", "EDUCATION",
        "HOME_LIVING", "AUTOMOTIVE", "FINANCIAL_SERVICES", "B2B_PROFESSIONAL", "OTHER_LONGTAIL"
    ]
    out = df
    for c in required:
        if c not in out.columns:
            out = out.withColumn(c, F.lit(0.0))
        out = out.withColumn(f"share_{c.lower()}", safe_div(F.col(c), F.col("spend_amt_m")))
    return out


def build_personas(df: DataFrame) -> DataFrame:
    x = add_missing_spend_cols(df)

    primary_persona = (
        F.when((F.col("share_travel_mobility") > 0.30) | (F.col("xborder_spend_share_m") > 0.20), F.lit("TRAVEL_CENTRIC"))
         .when((F.col("premium_spend_share_m") > 0.25) & (F.col("avg_ticket_m") > 3000), F.lit("AFFLUENT_LIFESTYLE"))
         .when((F.col("online_spend_share_m") + F.col("in_app_spend_share_m") > 0.60) & (F.col("wallet_spend_share_m") > 0.15), F.lit("DIGITAL_NATIVE"))
         .when((F.col("share_everyday_essentials") > 0.45) & (F.col("avg_ticket_m") < 1500), F.lit("EVERYDAY_ANCHOR"))
         .when((F.col("share_food_dining") + F.col("share_entertainment_leisure") > 0.40), F.lit("LIFESTYLE_SOCIAL"))
         .when((F.col("food_delivery_spend_share_m") + F.col("ride_hailing_spend_m")/F.when(F.col("spend_amt_m") == 0, F.lit(1.0)).otherwise(F.col("spend_amt_m")) > 0.25), F.lit("EMERGING_MOBILE"))
         .when(F.col("business_proxy_spend_share_m") > 0.30, F.lit("BUSINESS_PROXY"))
         .when((F.col("top_merchant_share_m") > 0.50) & (F.col("merchant_entropy_m") < 1.5), F.lit("CONCENTRATED_LOYALIST"))
         .when(F.col("merchant_entropy_m") > 3.0, F.lit("DIVERSIFIED_EXPLORER"))
         .when(F.col("spend_regime_flag").isin("DORMANT", "LOW_VOLUME"), F.lit("DORMANT_LOW_ACTIVITY"))
         .otherwise(F.lit("EVERYDAY_ANCHOR"))
    )

    out = x.withColumn("primary_persona", primary_persona)

    # persona confidence
    out = out.withColumn(
        "persona_confidence",
        F.when(F.col("primary_persona") == "TRAVEL_CENTRIC", F.greatest(F.col("share_travel_mobility"), F.col("xborder_spend_share_m")))
         .when(F.col("primary_persona") == "AFFLUENT_LIFESTYLE", F.greatest(F.col("premium_spend_share_m"), safe_div(F.col("avg_ticket_m"), F.lit(10000.0))))
         .when(F.col("primary_persona") == "DIGITAL_NATIVE", F.greatest(F.col("online_spend_share_m") + F.col("in_app_spend_share_m"), F.col("wallet_spend_share_m")))
         .when(F.col("primary_persona") == "EVERYDAY_ANCHOR", F.col("share_everyday_essentials"))
         .when(F.col("primary_persona") == "LIFESTYLE_SOCIAL", F.col("share_food_dining") + F.col("share_entertainment_leisure"))
         .when(F.col("primary_persona") == "EMERGING_MOBILE", F.col("food_delivery_spend_share_m"))
         .otherwise(F.lit(0.50))
    )

    # overlays
    out = (
        out
        .withColumn("overlay_subscription_heavy", F.when(F.col("recurring_merchant_cnt_m") >= 3, 1).otherwise(0))
        .withColumn("overlay_wallet_dependent", F.when(F.col("wallet_spend_share_m") > 0.30, 1).otherwise(0))
        .withColumn("overlay_food_delivery_native", F.when(F.col("food_delivery_spend_share_m") > 0.15, 1).otherwise(0))
        .withColumn("overlay_cross_border_frequent", F.when(F.col("xborder_spend_share_m") > 0.15, 1).otherwise(0))
        .withColumn("overlay_healthcare_engaged", F.when(F.col("share_health_wellness") > 0.15, 1).otherwise(0))
        .withColumn("overlay_premium_retail_oriented", F.when(F.col("premium_spend_share_m") > 0.20, 1).otherwise(0))
        .withColumn("overlay_convenience_dependent", F.when(F.col("share_everyday_essentials") > 0.50, 1).otherwise(0))
        .withColumn("overlay_business_spend_proxy", F.when(F.col("business_proxy_spend_share_m") > 0.20, 1).otherwise(0))
        .withColumn("overlay_risk_monitor_flag",
            F.when((F.col("topup_spend_share_m") > 0.40) | (F.col("l1_migration_flag") == 1) | (F.col("mom_spend_growth") < -0.50), 1).otherwise(0)
        )
    )

    w = Window.partitionBy("cust_num").orderBy("txn_month")
    out = out.withColumn("prev_persona", F.lag("primary_persona").over(w))
    out = out.withColumn("persona_shift_flag", F.when((F.col("prev_persona").isNotNull()) & (F.col("prev_persona") != F.col("primary_persona")), 1).otherwise(0))

    return out


# ================================================================
# 18. USE-CASE VIEWS
# ================================================================

def build_use_case_views(df: DataFrame):
    campaign_view = (
        df.select(
            "cust_num", "txn_month", "primary_persona", "persona_confidence",
            "overlay_subscription_heavy", "overlay_wallet_dependent", "overlay_food_delivery_native",
            "overlay_cross_border_frequent", "overlay_healthcare_engaged", "overlay_premium_retail_oriented",
            "spend_regime_flag", "feature_quality_flag", "dominant_ecosystem",
            "food_delivery_spend_share_m", "premium_spend_share_m", "xborder_spend_share_m"
        )
    )

    clm_view = (
        df.filter(F.col("feature_quality_flag") == "GOOD")
          .select(
              "cust_num", "txn_month", "primary_persona", "persona_confidence",
              "spend_amt_m", "spend_amt_3m", "mom_spend_growth", "avg_ticket_m",
              "premium_spend_share_m", "xborder_spend_share_m", "merchant_entropy_m", "ecosystem_entropy_m"
          )
    )

    risk_view = (
        df.select(
            "cust_num", "txn_month", "primary_persona", "spend_regime_flag",
            "overlay_risk_monitor_flag", "topup_spend_share_m", "wallet_spend_share_m",
            "business_proxy_spend_share_m", "l1_migration_flag", "mom_spend_growth",
            "merchant_entropy_trend", "diversification_flag"
        )
    )

    return campaign_view, clm_view, risk_view


# ================================================================
# 19. PHASE 3 PIPELINE
# ================================================================

def run_phase3(feature_mart: DataFrame) -> DataFrame:
    persona_mart = build_personas(feature_mart)
    persona_mart = persona_mart.withColumn("_processed_dtm", F.current_timestamp()).withColumn("_phase", F.lit("PHASE3"))
    persona_mart.write.mode("overwrite").format("delta").saveAsTable(PHASE3_OUTPUT_TABLE)

    campaign_view, clm_view, risk_view = build_use_case_views(persona_mart)
    campaign_view.write.mode("overwrite").format("delta").saveAsTable(f"{PHASE3_OUTPUT_TABLE}_campaign_view")
    clm_view.write.mode("overwrite").format("delta").saveAsTable(f"{PHASE3_OUTPUT_TABLE}_clm_view")
    risk_view.write.mode("overwrite").format("delta").saveAsTable(f"{PHASE3_OUTPUT_TABLE}_risk_view")

    append_audit("PHASE3", {
        "output_table": PHASE3_OUTPUT_TABLE,
        "persona_count": persona_mart.count(),
        "campaign_view_table": f"{PHASE3_OUTPUT_TABLE}_campaign_view",
        "clm_view_table": f"{PHASE3_OUTPUT_TABLE}_clm_view",
        "risk_view_table": f"{PHASE3_OUTPUT_TABLE}_risk_view",
    })

    return persona_mart


# ================================================================
# 20. MAIN
# ================================================================

def main():
    txn_enriched = run_phase1()
    feature_mart = run_phase2(txn_enriched)
    persona_mart = run_phase3(feature_mart)
    return txn_enriched, feature_mart, persona_mart


# Execute
# txn_enriched_df, feature_mart_df, persona_mart_df = main()
