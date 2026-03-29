# CLAUDE.md — Card Spend Intelligence V4 (merchant_persona)

## Project Overview

This repo implements **Card Spend Intelligence V4** — a production PySpark framework for merchant taxonomy classification and customer persona derivation from credit card transaction data at **CardX/SCB Thailand**.

- **GitHub repo**: https://github.com/Sushil-tata/merchant_persona
- **Institution**: Standard Chartered Bank (SCB) / CardX Thailand
- **Platform**: Databricks (PySpark + Delta Lake)
- **Status**: V4 production-hardened and complete
- **Primary outputs**: Merchant taxonomy tags + customer persona scores + micro-segment labels

## What This Repo Does

Three-phase pipeline that transforms raw daily transaction data into actionable customer intelligence:

```
Phase 1: Transaction Taxonomy
    Raw transactions → 3-level merchant classification
    (120+ regex rules, 13 L1 × 42 L2 × 65+ L3 hierarchy)
         ↓
Phase 2: Feature Engineering
    Customer-month feature mart
    (110+ features: spend velocity, category mix, timing, recency)
         ↓
Phase 3: Persona Classification
    8 dimension scores → 10 personas → 16 micro-segments
```

---

## Phase 1: Transaction Taxonomy

### Merchant Classification Hierarchy

| Level | Count | Examples |
|---|---|---|
| L1 (Category) | 13 | FOOD_BEVERAGE, TRAVEL, RETAIL, HEALTH, ENTERTAINMENT, ... |
| L2 (Subcategory) | 42 | RESTAURANTS, FAST_FOOD, COFFEE_SHOPS, AIRLINES, HOTELS, ... |
| L3 (Merchant type) | 65+ | GRAB_FOOD, MCDONALDS, STARBUCKS, THAI_AIRWAYS, ... |

### Classification Engine
- **120+ regex rules** matching on merchant name, MCC code, and merchant description
- **Thailand-focused**: Thai merchant name patterns (Thai script + romanized), Thai-specific chains (Central, The Mall, Big C, Lotus's, PTT, BTS/MRT)
- **Fallback logic**: L1 → L2 → L3 progressively — if L3 regex fails, L2 is retained; if L2 fails, L1 is assigned via MCC lookup
- Rule file is maintainable — add new merchants by appending regex patterns without touching pipeline logic

### Source Table
```
cdx_mdz_prd.cdx_curated_crcard_acl_db.crcard_txn_dly
```
Daily credit card transaction feed. Key columns: `cust_id`, `txn_dt`, `txn_amt_thb`, `merchant_name`, `merchant_mcc`, `merchant_desc`, `txn_type`.

---

## Phase 2: Feature Engineering (110+ Features)

Customer-month level feature mart aggregated from taxonomy-tagged transactions.

| Feature Family | Count | Examples |
|---|---|---|
| Spend velocity | ~20 | Total spend, MOM growth, spend acceleration |
| Category shares | ~30 | % spend on FOOD, % on TRAVEL, % on RETAIL |
| Merchant diversity | ~10 | Unique merchant count, HHI concentration index |
| Timing patterns | ~15 | Weekend ratio, late-night ratio, payday spike index |
| Recency/frequency | ~15 | Days since last txn, avg txns per week |
| THB-calibrated tiers | ~20 | Spend quintile (THB), premium merchant flag (>500 THB avg ticket) |

All features are **PIT-safe** (computed as of month-end snapshot). No future data leakage.

### THB Calibration
- All monetary thresholds are in Thai Baht (THB)
- Spend tiers calibrated to Thai income/spend norms (not USD-converted equivalents)
- Premium threshold: avg ticket > 500 THB; luxury threshold: avg ticket > 2,000 THB

---

## Phase 3: Persona Classification

### 8 Dimension Scores (0–100 each)
1. **Lifestyle Score** — discretionary vs. essential spend balance
2. **Mobility Score** — travel, transport, fuel spend intensity
3. **Digital Score** — e-commerce, streaming, digital service usage
4. **Dining Score** — restaurant, food delivery, café frequency
5. **Health & Wellness Score** — pharmacy, gym, hospital spend
6. **Premium Score** — luxury merchant, high-ticket transaction share
7. **Engagement Score** — transaction frequency and recency
8. **Financial Sophistication Score** — investment platform, insurance, FX usage

### 10 Customer Personas

| Persona | Key Dimensions | Profile |
|---|---|---|
| URBAN_PROFESSIONAL | High Digital, High Dining | City-based, frequent diner, heavy app user |
| FREQUENT_TRAVELER | High Mobility | Regular flights, hotels, international spend |
| FAMILY_SPENDER | High Lifestyle, Low Premium | Supermarket, school, family activities |
| PREMIUM_SHOPPER | High Premium, High Lifestyle | Luxury retail, fine dining, premium brands |
| HEALTH_CONSCIOUS | High Health | Pharmacy, gym, organic/health food |
| DIGITAL_NATIVE | High Digital, High Engagement | Online-first, streaming, digital payments |
| CONSERVATIVE_SPENDER | Low scores across | Low frequency, essential categories only |
| EMERGING_AFFLUENT | Rising Premium + Digital | Upward trajectory on premium/digital scores |
| CASH_CONVERTER | Low Engagement | Card used rarely, mostly ATM/cash |
| THIN | Very Low Engagement | Insufficient transaction history for scoring |

### 16 Micro-Segments
Each persona is split into 2 micro-segments based on spend tier (STANDARD vs. PREMIUM within persona) or behavioral sub-pattern (e.g., FREQUENT_TRAVELER → DOMESTIC_TRAVELER vs. INTERNATIONAL_TRAVELER).

---

## Output Tables (Delta Lake)

| Table | Description |
|---|---|
| `tmp.card_spend_txn_taxonomy_v4` | Transaction-level with L1/L2/L3 tags |
| `tmp.card_spend_feature_mart_v4` | Customer-month feature matrix |
| `tmp.card_spend_persona_v4` | Customer-month persona + micro-segment + 8 dimension scores |
| `tmp.card_spend_pipeline_audit_v4` | Pipeline run metadata, row counts, quality flags |

All tables are Delta Lake format with time-travel enabled.

---

## Main Files

| File | Size | Description |
|---|---|---|
| `build_card_spend_v4_production.py` | 2,669 lines | Master build script — generates a 32-cell production Databricks notebook |
| `Card_Spend_Intelligence_V4_Master_Notebook.ipynb` | — | The generated 32-cell Databricks notebook (ready to import) |

### How the Build Script Works
`build_card_spend_v4_production.py` is a **notebook generator** — it constructs the full 32-cell Databricks notebook programmatically. This pattern allows:
- Version-controlled notebook generation (no manual notebook editing)
- Parameterized builds (swap table names, date ranges, config)
- Clean diff history in git

To regenerate the notebook: `python build_card_spend_v4_production.py`

---

## Key Design Decisions

1. **Regex taxonomy (not ML classification)**: Merchant classification uses deterministic regex rules, not ML. This ensures interpretability, auditability, and easy maintenance when new merchants appear. ML would require labeled data that's hard to maintain.
2. **PIT-safe customer-month mart**: All features are computed as of month-end. No look-forward. This makes the mart safe for training models with any future label window.
3. **8 scores → personas (not direct clustering)**: Dimension scores are computed first, then personas are derived from score combinations. This is more interpretable than raw clustering and allows business users to adjust persona definitions without retraining.
4. **Thailand-specific calibration**: Thai spend patterns differ significantly from global norms. All thresholds (ticket sizes, category shares, spend tiers) are calibrated on Thai CardX data, not international benchmarks.
5. **`tmp` schema for outputs**: Using `tmp.` tables allows testing without touching production schemas. Promote to prod schema after UAT.

---

## Current Status

**V4 production-hardened and complete.**

| Component | Status |
|---|---|
| Phase 1: Transaction Taxonomy | Complete (120+ rules, 3-level hierarchy) |
| Phase 2: Feature Engineering | Complete (110+ features) |
| Phase 3: Persona Classification | Complete (8 dims, 10 personas, 16 micro-segments) |
| Output Delta tables | Complete (4 tables) |
| Pipeline audit logging | Complete |
| Databricks notebook (32 cells) | Complete |

---

## What's Next (Picking Up Where Left Off)

1. **Deploy to Databricks**: Import `Card_Spend_Intelligence_V4_Master_Notebook.ipynb` into Databricks workspace; configure cluster (need PySpark 3.x + Delta Lake); run full pipeline on 3-month data sample first
2. **Connect to NBA agent**: The V4 persona output (`tmp.card_spend_persona_v4`) is designed as input to the NBA decision agent in `claude_DA2` — wire persona + dimension scores into the NBA feature vector for persona-driven action recommendations
3. **Validate taxonomy coverage**: Run `pipeline_audit_v4` and check % of transactions with L3 classification vs. fallback to L2/L1 — target >80% L3 coverage; add regex rules for uncovered merchant patterns
4. **Calibrate persona thresholds**: Initial dimension score thresholds are heuristic — validate against known customer segments from CRM; adjust PREMIUM_SHOPPER and EMERGING_AFFLUENT thresholds based on actual spend distribution

---

## Important Context for Next Session

- **Source table is daily**: `crcard_txn_dly` is a daily append table. The pipeline aggregates to customer-month. Ensure the run date logic correctly bounds the month window.
- **Thai script in merchant names**: Some merchant names are in Thai Unicode. The regex rules handle both Thai and romanized forms. Do not convert merchant names to ASCII before matching — this will break Thai-script rules.
- **`tmp` schema**: Outputs are in `tmp.` which may have retention policies in Databricks. Before production go-live, confirm schema permissions and move to a persistent schema.
- **32-cell notebook structure**: The notebook is generated by `build_card_spend_v4_production.py`. Never edit the `.ipynb` directly — always edit the build script and regenerate.
- **NBA integration dependency**: The persona V4 output format was designed with `claude_DA2` NBA agent in mind. The persona field names and dimension score columns should not be renamed without also updating the NBA feature ingestion layer in `claude_DA2`.
- **V4 is the canonical version**: V1–V3 existed during development. V4 supersedes all prior versions. If you find references to earlier versions in notebooks or configs, use V4 exclusively.
