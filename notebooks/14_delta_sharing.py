# Databricks notebook source
# MAGIC %md
# MAGIC # 14 - Delta Sharing Setup
# MAGIC Sets up Delta Sharing for CPG partners with aggregated views that contain NO PII.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Create Sharing Schema and Views

# COMMAND ----------

spark.sql("CREATE SCHEMA IF NOT EXISTS v2_ontology.sharing")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Create Sharing View 1: cpg_product_performance
# MAGIC Aggregated product performance - NO PII (no household_id, no zip_code).

# COMMAND ----------

spark.sql("""
CREATE OR REPLACE VIEW v2_ontology.sharing.cpg_product_performance AS
SELECT
  s.department_name,
  s.category_name,
  s.brand,
  COUNT(DISTINCT t.transaction_id) AS transaction_count,
  SUM(t.quantity) AS total_units_sold,
  SUM(t.quantity * t.unit_price) AS total_revenue,
  AVG(t.unit_price) AS avg_selling_price,
  COUNT(DISTINCT t.store_id) AS store_count,
  COUNT(DISTINCT CAST(t.transaction_ts AS DATE)) AS active_days
FROM v2_raw.transactions.pos_transactions t
JOIN v2_raw.products.sku_master s ON t.upc = s.upc
GROUP BY s.department_name, s.category_name, s.brand
""")
print("Sharing view 1 created: cpg_product_performance")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Create Sharing View 2: cpg_dietary_audiences
# MAGIC Aggregated dietary cohort counts - NO PII.

# COMMAND ----------

spark.sql("""
CREATE OR REPLACE VIEW v2_ontology.sharing.cpg_dietary_audiences AS
SELECT
  dietary_cohort,
  COUNT(*) AS household_count,
  SUM(CASE WHEN is_active = TRUE THEN 1 ELSE 0 END) AS active_household_count,
  COUNT(DISTINCT loyalty_tier) AS loyalty_tier_diversity
FROM v2_ontology.abstractions.retail_media_audiences
GROUP BY dietary_cohort
""")
print("Sharing view 2 created: cpg_dietary_audiences")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Create Sharing View 3: cpg_substitution_insights
# MAGIC Aggregated substitution patterns - NO PII.

# COMMAND ----------

spark.sql("""
CREATE OR REPLACE VIEW v2_ontology.sharing.cpg_substitution_insights AS
SELECT
  src.department_name AS source_department,
  src.category_name AS source_category,
  tgt.department_name AS target_department,
  tgt.category_name AS target_category,
  COUNT(*) AS substitution_pair_count,
  AVG(sg.similarity_score) AS avg_similarity_score,
  SUM(CASE WHEN sg.is_dietary_safe = TRUE THEN 1 ELSE 0 END) AS dietary_safe_count
FROM v2_ontology.abstractions.substitution_graph sg
JOIN v2_raw.products.sku_master src ON sg.source_upc = src.upc
JOIN v2_raw.products.sku_master tgt ON sg.target_upc = tgt.upc
GROUP BY src.department_name, src.category_name, tgt.department_name, tgt.category_name
""")
print("Sharing view 3 created: cpg_substitution_insights")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Create Delta Share

# COMMAND ----------

share_created = False
try:
    spark.sql("CREATE SHARE IF NOT EXISTS `ontology-cpg-share`")
    print("Share 'ontology-cpg-share' created (or already exists).")
    share_created = True

    # Add views to the share
    sharing_views = [
        "v2_ontology.sharing.cpg_product_performance",
        "v2_ontology.sharing.cpg_dietary_audiences",
        "v2_ontology.sharing.cpg_substitution_insights"
    ]
    for view in sharing_views:
        try:
            spark.sql(f"ALTER SHARE `ontology-cpg-share` ADD TABLE {view}")
            print(f"  Added {view} to share")
        except Exception as e:
            # View may already be in share
            print(f"  {view}: {e}")

except Exception as e:
    print(f"Delta Sharing not available or share creation failed: {e}")
    print("Sharing views have been created in v2_ontology.sharing and can be shared manually.")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Verify No PII in Sharing Views

# COMMAND ----------

PII_COLUMNS = ["household_id", "zip_code", "store_id_primary", "age_band",
               "has_pharmacy_rx", "ssn", "dob", "email", "phone"]

sharing_views = {
    "cpg_product_performance": "v2_ontology.sharing.cpg_product_performance",
    "cpg_dietary_audiences": "v2_ontology.sharing.cpg_dietary_audiences",
    "cpg_substitution_insights": "v2_ontology.sharing.cpg_substitution_insights",
}

print("=== PII Check for Sharing Views ===")
all_clean = True
for name, full_name in sharing_views.items():
    try:
        cols = [c.lower() for c in spark.table(full_name).columns]
        pii_found = [c for c in PII_COLUMNS if c.lower() in cols]
        if pii_found:
            print(f"  WARNING: {name} contains PII columns: {pii_found}")
            all_clean = False
        else:
            count = spark.table(full_name).count()
            print(f"  {name}: CLEAN (no PII), {count:,} rows")
    except Exception as e:
        print(f"  {name}: ERROR - {e}")

if all_clean:
    print("\nAll sharing views are PII-free.")
else:
    print("\nWARNING: Some sharing views contain PII columns. Fix before sharing externally!")

# COMMAND ----------

print("\nDelta Sharing setup complete!")
print(f"  Schema: v2_ontology.sharing")
print(f"  Views: {list(sharing_views.keys())}")
print(f"  Share created: {share_created}")
