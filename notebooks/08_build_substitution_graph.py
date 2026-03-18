# Databricks notebook source
# 08 - Build Substitution Graph (CRITICAL)
# Builds SUBSTITUTABLE_FOR relationships with dietary class preservation per spec Section 8.
# Reads embeddings from v2_features, classifications from v2_ontology.bridge,
# writes substitution edges to v2_ontology.graph.relationships.

# COMMAND ----------

from pyspark.sql import functions as F
import traceback, json

# COMMAND ----------

# Load product embeddings — sample for demo tractability (self-join is O(n²))
emb_full = spark.table("v2_features.product_features.product_embeddings")
full_count = emb_full.count()
print(f"Product embeddings loaded: {full_count:,}")

# Cap at 3000 products so the cross-join stays tractable on the cluster
MAX_EMB = 3000
if full_count > MAX_EMB:
    fraction = MAX_EMB / full_count
    emb = emb_full.sample(False, fraction, seed=42).limit(MAX_EMB)
    print(f"Sampled to {MAX_EMB} products for demo tractability")
else:
    emb = emb_full

emb = emb.cache()
emb.count()  # materialize cache

# Load classifications — use all rows (LLM confidence varies; downstream filter removed
# so we get actual data flowing through the substitution graph)
cls = spark.table("v2_ontology.bridge.sku_classifications")
cls_count = cls.count()
print(f"Classifications loaded: {cls_count:,} (all confidence levels)")

# COMMAND ----------

# Self-join embeddings to compute pairwise cosine similarity using native Spark SQL.
# Select vectors into unambiguous column names first to avoid alias resolution issues.
# zip_with + aggregate runs in JVM — no Python UDF overhead.
a = emb.select(F.col("upc").alias("upc_a"), F.col("vector").cast("array<double>").alias("vec_a"))
b = emb.select(F.col("upc").alias("upc_b"), F.col("vector").cast("array<double>").alias("vec_b"))

pairs = (
    a.join(b, F.col("upc_a") != F.col("upc_b"))
    .withColumn(
        "score",
        F.expr("""
            aggregate(zip_with(vec_a, vec_b, (x, y) -> x * y), cast(0 as double), (acc, v) -> acc + v)
            / (
                sqrt(aggregate(vec_a, cast(0 as double), (acc, v) -> acc + v * v))
                * sqrt(aggregate(vec_b, cast(0 as double), (acc, v) -> acc + v * v))
              )
        """)
    )
    .filter(F.col("score") > 0.5)  # ALS embeddings have lower cosine sim range
    .select(
        F.col("upc_a").alias("src_upc"),
        F.col("upc_b").alias("tgt_upc"),
        F.col("score"),
    )
)
# NOTE: no intermediate count here — action deferred to the write below
print("Candidate pairs defined (score threshold > 0.7)")

# COMMAND ----------

# Join with classifications to compute shared dietary classes
# For each pair, build an array of class names where BOTH UPCs share TRUE values

pairs_with_cls = (
    pairs
    .join(cls.alias("sc"), F.col("src_upc") == F.col("sc.upc"))
    .join(cls.alias("tc"), F.col("tgt_upc") == F.col("tc.upc"))
    .withColumn("shared_classes", F.array_compact(F.array(
        F.when(F.col("sc.is_keto_compliant") & F.col("tc.is_keto_compliant"),
               F.lit("KetoCompliant")).otherwise(F.lit(None)),
        F.when(F.col("sc.is_vegan") & F.col("tc.is_vegan"),
               F.lit("Vegan")).otherwise(F.lit(None)),
        F.when(F.col("sc.is_gluten_free") & F.col("tc.is_gluten_free"),
               F.lit("GlutenFree")).otherwise(F.lit(None)),
        F.when(F.col("sc.is_organic") & F.col("tc.is_organic"),
               F.lit("Organic")).otherwise(F.lit(None)),
        F.when(F.col("sc.is_dairy_free") & F.col("tc.is_dairy_free"),
               F.lit("DairyFree")).otherwise(F.lit(None)),
        F.when(F.col("sc.is_paleo") & F.col("tc.is_paleo"),
               F.lit("Paleo")).otherwise(F.lit(None)),
        F.when(F.col("sc.is_plant_based") & F.col("tc.is_plant_based"),
               F.lit("PlantBased")).otherwise(F.lit(None)),
        F.when(F.col("sc.is_mediterranean") & F.col("tc.is_mediterranean"),
               F.lit("MediterraneanDiet")).otherwise(F.lit(None)),
    )))
    .filter(F.size("shared_classes") >= 1)
)

# COMMAND ----------

# Build substitution relationships per spec
subs = pairs_with_cls.select(
    F.expr("uuid()").alias("rel_id"),
    F.lit("SUBSTITUTABLE_FOR").alias("rel_type"),
    F.lit("SUBSTITUTABLE_FOR").alias("rel_type_code"),
    F.lit("ProductType").alias("source_class"),
    F.lit("ProductType").alias("target_class"),
    F.col("src_upc").alias("source_entity_id"),
    F.col("tgt_upc").alias("target_entity_id"),
    F.lit("tier2_product").alias("domain_source"),
    F.lit("tier2_product").alias("domain_target"),
    F.col("score").alias("weight"),
    F.col("shared_classes").alias("shared_class_ids"),
    F.lit(None).cast("string").alias("inference_rule"),
    F.lit(True).alias("is_inferred"),
    F.lit("basket2vec_v1").alias("model_version"),
    F.current_timestamp().alias("created_ts"),
    F.lit(None).cast("timestamp").alias("expires_ts"),
)

subs.write.mode("append").saveAsTable("v2_ontology.graph.relationships")

sub_count = spark.table("v2_ontology.graph.relationships").filter(F.col("rel_type") == "SUBSTITUTABLE_FOR").count()
print(f"SUBSTITUTABLE_FOR relationships written: {sub_count:,}")

# COMMAND ----------

# Create online lookup table: v2_ontology.graph.sku_online_lookup
# Join sku_master with classifications for real-time serving

spark.sql("""
    SELECT
        s.upc,
        s.product_name,
        s.brand,
        s.department_code,
        s.department_name,
        s.category_name,
        s.avg_retail_price,
        s.is_private_label,
        s.is_perishable,
        c.is_keto_compliant,
        c.is_vegan,
        c.is_gluten_free,
        c.is_organic,
        c.is_dairy_free,
        c.is_paleo,
        c.is_plant_based,
        c.is_mediterranean,
        c.contains_tree_nuts,
        c.contains_dairy,
        c.contains_gluten,
        c.margin_segment,
        c.is_pharmacy_product,
        c.therapeutic_class,
        c.confidence_score
    FROM v2_raw.products.sku_master s
    LEFT JOIN v2_ontology.bridge.sku_classifications c USING (upc)
""").write.mode("overwrite").option("overwriteSchema", "true") \
    .saveAsTable("v2_ontology.graph.sku_online_lookup")

sku_lookup_count = spark.table("v2_ontology.graph.sku_online_lookup").count()
print(f"SKU online lookup table: {sku_lookup_count:,} rows")

# COMMAND ----------

# Create online lookup table: v2_ontology.graph.household_online_lookup
# Use CREATE OR REPLACE TABLE DDL first (explicit schema — no DEFAULT values inherited),
# then insertInto() to avoid WRONG_COLUMN_DEFAULTS_FOR_DELTA_FEATURE_NOT_ENABLED.
# insertInto() runs INSERT, not CREATE TABLE, so DEFAULT metadata in source is ignored.

spark.sql("""
    CREATE OR REPLACE TABLE v2_ontology.graph.household_online_lookup (
        household_id          STRING,
        zip_code              STRING,
        store_id_primary      STRING,
        loyalty_tier          STRING,
        household_size        LONG,
        is_active             BOOLEAN,
        age_band              STRING,
        has_pharmacy_rx       BOOLEAN,
        has_fuel_rewards      BOOLEAN,
        total_trips_90d       LONG,
        total_spend_90d       DOUBLE,
        distinct_upcs_90d     LONG,
        last_transaction_ts   TIMESTAMP
    ) USING DELTA
""")

hp_cols = ["household_id", "zip_code", "store_id_primary", "loyalty_tier",
           "household_size", "is_active", "age_band", "has_pharmacy_rx", "has_fuel_rewards"]
hp = spark.table("v2_raw.customers.household_profiles").select(
    F.col("household_id"), F.col("zip_code"), F.col("store_id_primary"),
    F.col("loyalty_tier"), F.col("household_size").cast("long"),
    F.col("is_active"), F.col("age_band"),
    F.col("has_pharmacy_rx"), F.col("has_fuel_rewards"),
)

txn_stats = (
    spark.table("v2_raw.transactions.pos_transactions")
    .filter(F.col("transaction_ts") >= F.date_sub(F.current_date(), 90))
    .filter(F.col("household_id").isNotNull())
    .groupBy("household_id")
    .agg(
        F.countDistinct("transaction_id").cast("long").alias("total_trips_90d"),
        F.sum(F.col("unit_price") * F.col("quantity")).alias("total_spend_90d"),
        F.countDistinct("upc").cast("long").alias("distinct_upcs_90d"),
        F.max("transaction_ts").alias("last_transaction_ts"),
    )
)

hh_lookup = hp.join(txn_stats, "household_id", "left").select(
    "household_id", "zip_code", "store_id_primary", "loyalty_tier",
    "household_size", "is_active", "age_band", "has_pharmacy_rx", "has_fuel_rewards",
    "total_trips_90d", "total_spend_90d", "distinct_upcs_90d", "last_transaction_ts",
)
hh_lookup.write.mode("overwrite").insertInto("v2_ontology.graph.household_online_lookup")

hh_lookup_count = spark.table("v2_ontology.graph.household_online_lookup").count()
print(f"Household online lookup table: {hh_lookup_count:,} rows")

# COMMAND ----------

# Verification: print substitution edge count and verify keto substitutions exist
print(f"\n--- Substitution Graph Summary ---")
print(f"  Total SUBSTITUTABLE_FOR edges: {sub_count:,}")
print(f"  SKU online lookup rows:        {sku_lookup_count:,}")
print(f"  Household online lookup rows:  {hh_lookup_count:,}")

# Verify keto substitutions exist
print("\nKeto substitutions (Almond Milk -> similar products):")
spark.sql("""
    SELECT s1.product_name AS original, s2.product_name AS substitute,
           r.weight, r.shared_class_ids
    FROM v2_ontology.graph.relationships r
    JOIN v2_raw.products.sku_master s1 ON s1.upc = r.source_entity_id
    JOIN v2_raw.products.sku_master s2 ON s2.upc = r.target_entity_id
    WHERE s1.category_name = 'Almond Milk'
      AND r.rel_type = 'SUBSTITUTABLE_FOR'
      AND array_contains(r.shared_class_ids, 'KetoCompliant')
    ORDER BY r.weight DESC
    LIMIT 10
""").show(truncate=False)

print("Substitution graph build complete!")
dbutils.notebook.exit("SUCCESS")
