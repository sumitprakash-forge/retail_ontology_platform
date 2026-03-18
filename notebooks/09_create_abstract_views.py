# Databricks notebook source
# MAGIC %md
# MAGIC # 09 - Create Abstract Views (Layer 3)
# MAGIC Creates 7 abstract views in v2_ontology.abstractions that span multiple
# MAGIC ontology domains, providing business-ready interfaces for agents and BI tools.
# MAGIC Also creates a view_registry table tracking all views.

# COMMAND ----------

spark.sql("CREATE SCHEMA IF NOT EXISTS v2_ontology.abstractions")

# COMMAND ----------

# MAGIC %md
# MAGIC ## View 1: unified_inventory
# MAGIC JOIN inventory snapshots with SKU master for a single inventory surface.

# COMMAND ----------

spark.sql("""
CREATE OR REPLACE VIEW v2_ontology.abstractions.unified_inventory AS
SELECT
  i.upc,
  s.product_name,
  s.brand,
  s.department_name,
  i.node_id,
  i.node_type,
  i.on_hand_qty,
  i.reserved_qty,
  i.in_transit_qty,
  i.on_hand_qty - i.reserved_qty AS available_qty,
  i.reorder_point,
  i.snapshot_ts
FROM v2_raw.inventory.node_inventory_snapshots i
JOIN v2_raw.products.sku_master s ON i.upc = s.upc
""")
print("View 1 created: v2_ontology.abstractions.unified_inventory")

# COMMAND ----------

# MAGIC %md
# MAGIC ## View 2: dietary_sku_catalog
# MAGIC JOIN SKU master with AI classifications for dietary flags.

# COMMAND ----------

spark.sql("""
CREATE OR REPLACE VIEW v2_ontology.abstractions.dietary_sku_catalog AS
SELECT
  s.upc,
  s.product_name,
  s.brand,
  s.department_name,
  s.category_name,
  s.avg_retail_price,
  COALESCE(c.is_keto_compliant, FALSE) AS is_keto,
  COALESCE(c.is_vegan, FALSE) AS is_vegan,
  COALESCE(c.is_gluten_free, FALSE) AS is_gluten_free,
  COALESCE(c.is_organic, FALSE) AS is_organic,
  COALESCE(c.is_dairy_free, FALSE) AS is_dairy_free,
  COALESCE(c.is_paleo, FALSE) AS is_paleo,
  COALESCE(c.is_plant_based, FALSE) AS is_plant_based,
  COALESCE(c.margin_segment, 'Standard') AS margin_segment,
  c.confidence_score
FROM v2_raw.products.sku_master s
LEFT JOIN v2_ontology.bridge.sku_classifications c ON s.upc = c.upc
""")
print("View 2 created: v2_ontology.abstractions.dietary_sku_catalog")

# COMMAND ----------

# MAGIC %md
# MAGIC ## View 3: substitution_graph
# MAGIC Substitutable product pairs with dietary safety check.

# COMMAND ----------

spark.sql("""
CREATE OR REPLACE VIEW v2_ontology.abstractions.substitution_graph AS
SELECT
  r.source_entity_id AS source_upc,
  r.target_entity_id AS target_upc,
  src.product_name AS source_product_name,
  tgt.product_name AS target_product_name,
  r.weight AS similarity_score,
  r.shared_class_ids,
  -- is_dietary_safe: true if source and target share same keto/vegan/gf flags
  CASE
    WHEN COALESCE(sc.is_keto_compliant, FALSE) = COALESCE(tc.is_keto_compliant, FALSE)
     AND COALESCE(sc.is_vegan, FALSE) = COALESCE(tc.is_vegan, FALSE)
     AND COALESCE(sc.is_gluten_free, FALSE) = COALESCE(tc.is_gluten_free, FALSE)
    THEN TRUE
    ELSE FALSE
  END AS is_dietary_safe
FROM v2_ontology.graph.relationships r
JOIN v2_raw.products.sku_master src ON src.upc = r.source_entity_id
JOIN v2_raw.products.sku_master tgt ON tgt.upc = r.target_entity_id
LEFT JOIN v2_ontology.bridge.sku_classifications sc ON sc.upc = r.source_entity_id
LEFT JOIN v2_ontology.bridge.sku_classifications tc ON tc.upc = r.target_entity_id
WHERE r.rel_type = 'SUBSTITUTABLE_FOR'
""")
print("View 3 created: v2_ontology.abstractions.substitution_graph")

# COMMAND ----------

# MAGIC %md
# MAGIC ## View 4: retail_media_audiences
# MAGIC Household audience profiles with inferred dietary cohort.

# COMMAND ----------

spark.sql("""
CREATE OR REPLACE VIEW v2_ontology.abstractions.retail_media_audiences AS
WITH hh_dietary AS (
  SELECT
    t.household_id,
    SUM(CASE WHEN c.is_keto_compliant = TRUE THEN t.unit_price * t.quantity ELSE 0 END)
      / NULLIF(SUM(t.unit_price * t.quantity), 0) AS keto_share,
    SUM(CASE WHEN c.is_vegan = TRUE THEN t.unit_price * t.quantity ELSE 0 END)
      / NULLIF(SUM(t.unit_price * t.quantity), 0) AS vegan_share,
    SUM(CASE WHEN c.is_gluten_free = TRUE THEN t.unit_price * t.quantity ELSE 0 END)
      / NULLIF(SUM(t.unit_price * t.quantity), 0) AS gf_share
  FROM v2_raw.transactions.pos_transactions t
  JOIN v2_ontology.bridge.sku_classifications c ON t.upc = c.upc
  WHERE t.household_id IS NOT NULL
  GROUP BY t.household_id
)
SELECT
  hp.household_id,
  hp.zip_code,
  hp.store_id_primary,
  hp.loyalty_tier,
  hp.age_band,
  hp.has_pharmacy_rx,
  hp.is_active,
  CASE
    WHEN d.keto_share > 0.35 THEN 'KetoDieter'
    WHEN d.vegan_share > 0.35 THEN 'Vegan'
    WHEN d.gf_share > 0.35 THEN 'GlutenFreePreferred'
    ELSE 'General'
  END AS dietary_cohort
FROM v2_raw.customers.household_profiles hp
LEFT JOIN hh_dietary d ON hp.household_id = d.household_id
""")
print("View 4 created: v2_ontology.abstractions.retail_media_audiences")

# COMMAND ----------

# MAGIC %md
# MAGIC ## View 5: margin_aware_substitution
# MAGIC Substitution graph enriched with pricing and margin delta.

# COMMAND ----------

spark.sql("""
CREATE OR REPLACE VIEW v2_ontology.abstractions.margin_aware_substitution AS
SELECT
  sg.source_upc,
  sg.target_upc,
  sg.source_product_name,
  sg.target_product_name,
  sg.similarity_score,
  sg.shared_class_ids,
  sg.is_dietary_safe,
  src_sku.avg_retail_price AS source_price,
  tgt_sku.avg_retail_price AS target_price,
  COALESCE(tgt_sku.avg_retail_price, 0) - COALESCE(src_sku.avg_retail_price, 0) AS margin_delta
FROM v2_ontology.abstractions.substitution_graph sg
JOIN v2_raw.products.sku_master src_sku ON sg.source_upc = src_sku.upc
JOIN v2_raw.products.sku_master tgt_sku ON sg.target_upc = tgt_sku.upc
""")
print("View 5 created: v2_ontology.abstractions.margin_aware_substitution")

# COMMAND ----------

# MAGIC %md
# MAGIC ## View 6: pharmacy_grocery_crosssell
# MAGIC Pharmacy prescriptions with food interactions for cross-sell opportunities.

# COMMAND ----------

spark.sql("""
CREATE OR REPLACE VIEW v2_ontology.abstractions.pharmacy_grocery_crosssell AS
SELECT
  hp.household_id,
  rx.drug_class,
  rx.drug_name,
  di.interaction_severity,
  di.food_interaction_detail,
  hp.zip_code,
  hp.store_id_primary
FROM v2_raw.pharmacy.prescriptions rx
JOIN v2_raw.pharmacy.drug_interactions di
  ON rx.drug_class = di.drug_class_1
  AND di.food_interaction_flag = TRUE
JOIN v2_raw.customers.household_profiles hp
  ON rx.household_id = hp.household_id
""")
print("View 6 created: v2_ontology.abstractions.pharmacy_grocery_crosssell")

# COMMAND ----------

# MAGIC %md
# MAGIC ## View 7: atrisk_customer_radar
# MAGIC Active customers with low transaction frequency indicating churn risk.

# COMMAND ----------

spark.sql("""
CREATE OR REPLACE VIEW v2_ontology.abstractions.atrisk_customer_radar AS
WITH txn_counts AS (
  SELECT
    household_id,
    COUNT(*) AS transaction_count,
    MAX(transaction_ts) AS last_transaction_ts
  FROM v2_raw.transactions.pos_transactions
  WHERE household_id IS NOT NULL
  GROUP BY household_id
)
SELECT
  hp.household_id,
  hp.loyalty_tier,
  hp.is_active,
  hp.store_id_primary,
  COALESCE(tc.transaction_count, 0) AS transaction_count,
  tc.last_transaction_ts,
  DATEDIFF(CURRENT_DATE(), CAST(tc.last_transaction_ts AS DATE)) AS days_since_last_purchase
FROM v2_raw.customers.household_profiles hp
LEFT JOIN txn_counts tc ON hp.household_id = tc.household_id
WHERE hp.is_active = TRUE
  AND (tc.transaction_count IS NULL OR tc.transaction_count < 5)
""")
print("View 7 created: v2_ontology.abstractions.atrisk_customer_radar")

# COMMAND ----------

# MAGIC %md
# MAGIC ## View Registry
# MAGIC Tracking table for all abstract views.

# COMMAND ----------

from pyspark.sql import Row
from datetime import datetime

view_registry_data = [
    Row(view_name="unified_inventory", schema_name="v2_ontology.abstractions",
        description="Single inventory surface joining node snapshots with SKU master",
        source_tables="v2_raw.inventory.node_inventory_snapshots, v2_raw.products.sku_master",
        created_ts=datetime.utcnow(), created_by="ontology_bootstrap", is_active=True),
    Row(view_name="dietary_sku_catalog", schema_name="v2_ontology.abstractions",
        description="SKU catalog enriched with AI dietary and allergen classifications",
        source_tables="v2_raw.products.sku_master, v2_ontology.bridge.sku_classifications",
        created_ts=datetime.utcnow(), created_by="ontology_bootstrap", is_active=True),
    Row(view_name="substitution_graph", schema_name="v2_ontology.abstractions",
        description="Pre-scored substitution pairs with dietary safety flag",
        source_tables="v2_ontology.graph.relationships, v2_raw.products.sku_master, v2_ontology.bridge.sku_classifications",
        created_ts=datetime.utcnow(), created_by="ontology_bootstrap", is_active=True),
    Row(view_name="retail_media_audiences", schema_name="v2_ontology.abstractions",
        description="Household audience profiles with inferred dietary cohort",
        source_tables="v2_raw.customers.household_profiles, v2_raw.transactions.pos_transactions, v2_ontology.bridge.sku_classifications",
        created_ts=datetime.utcnow(), created_by="ontology_bootstrap", is_active=True),
    Row(view_name="margin_aware_substitution", schema_name="v2_ontology.abstractions",
        description="Substitution graph enriched with pricing and margin delta",
        source_tables="v2_ontology.abstractions.substitution_graph, v2_raw.products.sku_master",
        created_ts=datetime.utcnow(), created_by="ontology_bootstrap", is_active=True),
    Row(view_name="pharmacy_grocery_crosssell", schema_name="v2_ontology.abstractions",
        description="Pharmacy prescriptions with food interactions for cross-sell",
        source_tables="v2_raw.pharmacy.prescriptions, v2_raw.pharmacy.drug_interactions, v2_raw.customers.household_profiles",
        created_ts=datetime.utcnow(), created_by="ontology_bootstrap", is_active=True),
    Row(view_name="atrisk_customer_radar", schema_name="v2_ontology.abstractions",
        description="Active customers with low transaction frequency indicating churn risk",
        source_tables="v2_raw.customers.household_profiles, v2_raw.transactions.pos_transactions",
        created_ts=datetime.utcnow(), created_by="ontology_bootstrap", is_active=True),
]

spark.createDataFrame(view_registry_data) \
    .write.mode("overwrite") \
    .saveAsTable("v2_ontology.abstractions.view_registry")

print("View registry created with 7 entries")

# COMMAND ----------

# Verify all views
print("\n=== Verification ===")
views = [
    "unified_inventory", "dietary_sku_catalog", "substitution_graph",
    "retail_media_audiences", "margin_aware_substitution",
    "pharmacy_grocery_crosssell", "atrisk_customer_radar"
]
for v in views:
    try:
        count = spark.table(f"v2_ontology.abstractions.{v}").count()
        print(f"  {v}: {count:,} rows")
    except Exception as e:
        print(f"  {v}: ERROR - {e}")

print("\nView registry:")
spark.table("v2_ontology.abstractions.view_registry").show(truncate=False)
