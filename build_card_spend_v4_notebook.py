import json

def md(text):
    return {
        "cell_type": "markdown",
        "metadata": {},
        "source": text.splitlines(True)
    }

def code(text):
    return {
        "cell_type": "code",
        "execution_count": None,
        "metadata": {},
        "outputs": [],
        "source": text.splitlines(True)
    }

cells = []

cells.append(md("""
# Card Spend Intelligence V4 — Master Pipeline

End-to-end pipeline:

Transactions  
→ Merchant Normalization  
→ Merchant Intelligence  
→ Taxonomy Reconciliation  
→ Behavioral Tags  
→ Customer Feature Mart  
→ Entropy / Concentration  
→ Rolling Features  
→ Persona Engine  
→ Campaign / Risk / CLM Output
"""))

cells.append(code("""
from pyspark.sql import functions as F
from pyspark.sql.window import Window
from pyspark.sql.types import *
"""))

cells.append(code("""
# CONFIG

SRC_TABLE = "transactions_table"

PHASE1_TABLE = "card_spend_txn_enriched"
PHASE2_TABLE = "card_spend_feature_mart"
PHASE3_TABLE = "card_spend_persona"

"""))

cells.append(code("""
txn = spark.table(SRC_TABLE)

txn = (
    txn
    .withColumn("txn_date", F.to_date("TXN_DT"))
    .withColumn("txn_month", F.date_trunc("month","TXN_DT"))
)
"""))

cells.append(code("""
# Merchant normalization

def norm(col):
    return F.lower(F.trim(F.regexp_replace(col,"\\\\s+"," ")))

txn = txn.withColumn("merchant_norm", norm(F.col("MCHT_NM")))
"""))

cells.append(code("""
# Channel tags

txn = txn.withColumn(
    "channel_tag",
    F.when(F.col("ONLN_FLAG")=="Y","ONLINE")
     .when(F.col("ONLN_FLAG")=="N","OFFLINE")
     .otherwise("UNKNOWN")
)
"""))

cells.append(code("""
# Wallet / topup detection

wallet_patterns = "truemoney|linepay|rabbit line pay|shopeepay"

txn = txn.withColumn(
    "wallet_topup_flag",
    F.when(F.col("merchant_norm").rlike(wallet_patterns),1).otherwise(0)
)
"""))

cells.append(code("""
# Behavioral tags

txn = txn.withColumn(
    "txn_size_tier",
    F.when(F.col("TXN_AMT") < 100,"MICRO")
     .when(F.col("TXN_AMT") < 500,"SMALL")
     .when(F.col("TXN_AMT") < 2000,"STANDARD")
     .when(F.col("TXN_AMT") < 10000,"MID")
     .otherwise("LARGE")
)
"""))

cells.append(code("""
# Customer-month aggregation

cust_month = (
    txn
    .groupBy("CUST_NUM","txn_month")
    .agg(
        F.count("*").alias("txn_cnt_m"),
        F.sum("TXN_AMT").alias("spend_amt_m"),
        F.avg("TXN_AMT").alias("avg_ticket_m"),
        F.approx_count_distinct("merchant_norm").alias("uniq_merchants_m"),
        F.sum(F.when(F.col("channel_tag")=="ONLINE",F.col("TXN_AMT")).otherwise(0)).alias("online_spend_m"),
        F.sum("wallet_topup_flag").alias("wallet_txn_cnt")
    )
)
"""))

cells.append(code("""
# Merchant entropy

merchant_month = (
    txn.groupBy("CUST_NUM","txn_month","merchant_norm")
    .agg(F.sum("TXN_AMT").alias("merchant_spend"))
)

w = Window.partitionBy("CUST_NUM","txn_month")

merchant_month = merchant_month.withColumn(
    "total_spend",
    F.sum("merchant_spend").over(w)
)

merchant_month = merchant_month.withColumn(
    "p",
    F.col("merchant_spend")/F.col("total_spend")
)

entropy = merchant_month.groupBy("CUST_NUM","txn_month").agg(
    F.sum(-F.col("p")*F.log2("p")).alias("merchant_entropy")
)

cust_month = cust_month.join(entropy,["CUST_NUM","txn_month"],"left")
"""))

cells.append(code("""
# Rolling features

w = Window.partitionBy("CUST_NUM").orderBy("txn_month")

cust_month = cust_month.withColumn(
    "spend_prev_m",
    F.lag("spend_amt_m").over(w)
)

cust_month = cust_month.withColumn(
    "spend_velocity",
    F.col("spend_amt_m") - F.col("spend_prev_m")
)
"""))

cells.append(code("""
# Persona engine

cust_month = cust_month.withColumn(
    "primary_persona",
    F.when(F.col("online_spend_m") > 0.6 * F.col("spend_amt_m"),"DIGITAL_NATIVE")
     .when(F.col("merchant_entropy") < 1.2,"LOYAL_SPENDER")
     .when(F.col("avg_ticket_m") > 5000,"PREMIUM_AFFLUENT")
     .otherwise("EVERYDAY_ANCHOR")
)
"""))

cells.append(code("""
cust_month.write.mode("overwrite").saveAsTable(PHASE3_TABLE)
"""))

notebook = {
    "cells": cells,
    "metadata": {
        "kernelspec":{
            "display_name":"Python 3",
            "language":"python",
            "name":"python3"
        },
        "language_info":{
            "name":"python",
            "version":"3.11"
        }
    },
    "nbformat":4,
    "nbformat_minor":5
}

with open("Card_Spend_Intelligence_V4_Master_Notebook.ipynb","w") as f:
    json.dump(notebook,f,indent=2)

print("Notebook generated successfully")
