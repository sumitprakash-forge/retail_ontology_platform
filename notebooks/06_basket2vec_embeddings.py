# Databricks notebook source
# 06 - Basket2Vec Embeddings
# Builds product and household embeddings from basket co-occurrence per spec Section 8.
# Uses MLlib ALS (Alternating Least Squares) on implicit basket feedback.
# ALS is fully whitelisted on DBR 17.3 Single User + UC compatible.
# Reads transactions from v2_raw, writes embeddings to v2_features.

# COMMAND ----------

from pyspark.sql import functions as F
from pyspark.sql.types import *
from pyspark.ml.recommendation import ALS
from pyspark.ml.feature import StringIndexer

# COMMAND ----------

# Load last 90 days of transactions
txns = (
    spark.table("v2_raw.transactions.pos_transactions")
    .filter(F.col("transaction_ts") >= F.date_sub(F.current_date(), 90))
    .select("household_id", "upc")
    .filter(F.col("household_id").isNotNull())
)

total_txns = txns.count()
print(f"Transactions (last 90 days): {total_txns:,}")

# COMMAND ----------

# Build implicit feedback: household x UPC purchase frequency
# ALS uses integer IDs — index household_id and upc strings to integers
hh_indexer = StringIndexer(inputCol="household_id", outputCol="hh_idx", handleInvalid="skip")
upc_indexer = StringIndexer(inputCol="upc", outputCol="upc_idx", handleInvalid="skip")

hh_model = hh_indexer.fit(txns)
upc_model = upc_indexer.fit(txns)

indexed = hh_model.transform(upc_model.transform(txns))

# Purchase count as implicit feedback signal
ratings = (
    indexed
    .groupBy("hh_idx", "upc_idx", "household_id", "upc")
    .agg(F.count("*").cast("float").alias("purchase_count"))
)

n_households = ratings.select("hh_idx").distinct().count()
n_upcs = ratings.select("upc_idx").distinct().count()
print(f"Households: {n_households:,} | Unique UPCs: {n_upcs:,}")

# COMMAND ----------

# Train ALS model with implicit feedback (purchase counts)
# rank=128 gives 128-dim embeddings matching the spec
als = ALS(
    rank=128,
    maxIter=10,
    regParam=0.1,
    alpha=40.0,           # implicit feedback confidence scaling
    implicitPrefs=True,   # basket purchase = implicit signal
    userCol="hh_idx",
    itemCol="upc_idx",
    ratingCol="purchase_count",
    coldStartStrategy="drop",
    seed=42,
    numUserBlocks=50,
    numItemBlocks=50,
)

print("Training ALS model (rank=128, implicit feedback)...")
als_model = als.fit(ratings)
print("ALS model trained successfully!")

# COMMAND ----------

# Extract product (item) embeddings — itemFactors: id (int), features (array<float>)
item_factors = als_model.itemFactors  # id=upc_idx, features=128-dim vector

# Map upc_idx back to UPC string
upc_lookup = (
    ratings.select("upc_idx", "upc")
    .distinct()
    .withColumnRenamed("upc_idx", "id")
)

product_vectors = (
    item_factors
    .join(upc_lookup, "id")
    .select(
        F.col("upc"),
        F.col("features").alias("vector"),
        F.lit("als_basket2vec_v1").alias("embedding_model"),
        F.current_timestamp().alias("generated_ts"),
    )
)

print(f"Product embeddings: {product_vectors.count():,} UPCs")

# COMMAND ----------

# Ensure target schemas exist
spark.sql("CREATE SCHEMA IF NOT EXISTS v2_features.product_features")
spark.sql("CREATE SCHEMA IF NOT EXISTS v2_features.customer_features")

# COMMAND ----------

# Write product embeddings
product_vectors.write.mode("overwrite") \
    .option("overwriteSchema", "true") \
    .saveAsTable("v2_features.product_features.product_embeddings")

spark.sql("""
    ALTER TABLE v2_features.product_features.product_embeddings
    SET TBLPROPERTIES ('delta.enableChangeDataFeed' = 'true')
""")

product_count = spark.table("v2_features.product_features.product_embeddings").count()
print(f"Product embeddings written: {product_count:,}")

# COMMAND ----------

# Extract household (user) embeddings — userFactors: id (int), features (array<float>)
user_factors = als_model.userFactors  # id=hh_idx, features=128-dim vector

# Map hh_idx back to household_id string
hh_lookup = (
    ratings.select("hh_idx", "household_id")
    .distinct()
    .withColumnRenamed("hh_idx", "id")
)

household_vectors = (
    user_factors
    .join(hh_lookup, "id")
    .select(
        F.col("household_id"),
        F.col("features").alias("vector"),
        F.lit("als_basket2vec_v1_hh").alias("embedding_model"),
        F.current_timestamp().alias("generated_ts"),
    )
)

household_vectors.write.mode("overwrite") \
    .option("overwriteSchema", "true") \
    .saveAsTable("v2_features.customer_features.household_embeddings")

spark.sql("""
    ALTER TABLE v2_features.customer_features.household_embeddings
    SET TBLPROPERTIES ('delta.enableChangeDataFeed' = 'true')
""")

hh_count = spark.table("v2_features.customer_features.household_embeddings").count()
print(f"Household embeddings written: {hh_count:,}")

# COMMAND ----------

print("\n--- Embedding Summary ---")
print(f"  Product embeddings:   {product_count:,}")
print(f"  Household embeddings: {hh_count:,}")
print(f"  Vector dimensions:    128 (ALS rank)")
print(f"  Model:                als_basket2vec_v1 (MLlib ALS, implicit feedback)")
print("Basket2Vec ALS embedding generation complete!")
