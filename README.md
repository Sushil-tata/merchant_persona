# Card Spend Intelligence V4 - Merchant Persona Framework

Production-grade PySpark framework for credit card transaction taxonomy, merchant intelligence, feature engineering, and customer persona classification.

## Files

| File | Description |
|---|---|
| build_card_spend_v4_production.py | Builder script (2,669 lines) - generates the master notebook. Audited and hardened. |
| Card_Spend_Intelligence_V4_Master_Notebook.ipynb | Generated master notebook (32 cells) - the production deliverable. |
| build_card_spend_v4_notebook.py | Earlier builder version (V4 initial). Superseded by production version. |
| new_chat.py | V1-class modular reference script. Strict subset of V4 - retained for reference only. |

## Architecture

### 3-Phase Pipeline
1. Phase 1 - Transaction Taxonomy (Cells 1-13): Merchant normalization, token extraction, 120+ regex rules, 13 L1 x 42 L2 x 65+ L3 hierarchy
2. Phase 2 - Feature Engineering (Cells 14-22): 110+ customer-month features, merchant intelligence, rolling windows (3M/6M/12M)
3. Phase 3 - Persona Classification (Cells 23-31): 8 dimension scores, 10 personas, 16 micro-segments, confidence scoring

### Key Design Principles
- PIT-safe: All rolling windows prevent future data leakage
- Deterministic: row_number() over priority for dedup
- Thailand-focused: Thai merchant patterns, THB-calibrated tiers
- Delta Lake: All outputs written as managed Delta tables

## Usage

Generate notebook:
```
python3 build_card_spend_v4_production.py
```

Source table: cdx_mdz_prd.cdx_curated_crcard_acl_db.crcard_txn_dly

Output tables:
- tmp.card_spend_txn_taxonomy_v4
- tmp.card_spend_feature_mart_v4
- tmp.card_spend_persona_v4
- tmp.card_spend_pipeline_audit_v4
