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
# MAGIC Substitution graph enriched with dietary_fit_score × margin multiplier (PDF spec).

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
  COALESCE(tgt_sku.avg_retail_price, 0) - COALESCE(src_sku.avg_retail_price, 0) AS margin_delta,
  -- dietary_fit_score: 1.0 if same keto/vegan/gf flags; 0.5 if partial; 0.0 if conflict
  CASE
    WHEN COALESCE(tc.is_keto_compliant, FALSE) = COALESCE(sc.is_keto_compliant, FALSE)
     AND COALESCE(tc.is_vegan, FALSE) = COALESCE(sc.is_vegan, FALSE)
     AND COALESCE(tc.is_gluten_free, FALSE) = COALESCE(sc.is_gluten_free, FALSE)
    THEN 1.0
    WHEN COALESCE(tc.is_keto_compliant, FALSE) = COALESCE(sc.is_keto_compliant, FALSE)
    THEN 0.5
    ELSE 0.0
  END AS dietary_fit_score,
  -- margin_multiplier: ratio of target to source price (how much margin gain per unit)
  CASE
    WHEN COALESCE(src_sku.avg_retail_price, 0) > 0
    THEN COALESCE(tgt_sku.avg_retail_price, src_sku.avg_retail_price) / src_sku.avg_retail_price
    ELSE 1.0
  END AS margin_multiplier,
  -- composite_score: dietary_fit × margin_multiplier × similarity_score (PDF spec formula)
  CASE
    WHEN COALESCE(src_sku.avg_retail_price, 0) > 0
      AND COALESCE(tc.is_keto_compliant, FALSE) = COALESCE(sc.is_keto_compliant, FALSE)
      AND COALESCE(tc.is_vegan, FALSE) = COALESCE(sc.is_vegan, FALSE)
      AND COALESCE(tc.is_gluten_free, FALSE) = COALESCE(sc.is_gluten_free, FALSE)
    THEN sg.similarity_score
        * 1.0
        * (COALESCE(tgt_sku.avg_retail_price, src_sku.avg_retail_price) / src_sku.avg_retail_price)
    WHEN COALESCE(src_sku.avg_retail_price, 0) > 0
    THEN sg.similarity_score
        * 0.5
        * (COALESCE(tgt_sku.avg_retail_price, src_sku.avg_retail_price) / src_sku.avg_retail_price)
    ELSE sg.similarity_score * 0.5
  END AS composite_score
FROM v2_ontology.abstractions.substitution_graph sg
JOIN v2_raw.products.sku_master src_sku ON sg.source_upc = src_sku.upc
JOIN v2_raw.products.sku_master tgt_sku ON sg.target_upc = tgt_sku.upc
LEFT JOIN v2_ontology.bridge.sku_classifications sc ON sg.source_upc = sc.upc
LEFT JOIN v2_ontology.bridge.sku_classifications tc ON sg.target_upc = tc.upc
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
# MAGIC Detects trip frequency drop: current 30d trips vs prior 30d trips (PDF spec).

# COMMAND ----------

spark.sql("""
CREATE OR REPLACE VIEW v2_ontology.abstractions.atrisk_customer_radar AS
WITH recent_trips AS (
  SELECT
    household_id,
    COUNT(*) AS trips_last_30d,
    SUM(quantity * unit_price) AS spend_last_30d
  FROM v2_raw.transactions.pos_transactions
  WHERE household_id IS NOT NULL
    AND transaction_ts >= CURRENT_TIMESTAMP() - INTERVAL 30 DAYS
  GROUP BY household_id
),
prior_trips AS (
  SELECT
    household_id,
    COUNT(*) AS trips_prior_30d,
    SUM(quantity * unit_price) AS spend_prior_30d
  FROM v2_raw.transactions.pos_transactions
  WHERE household_id IS NOT NULL
    AND transaction_ts >= CURRENT_TIMESTAMP() - INTERVAL 60 DAYS
    AND transaction_ts <  CURRENT_TIMESTAMP() - INTERVAL 30 DAYS
  GROUP BY household_id
),
all_trips AS (
  SELECT
    household_id,
    COUNT(*) AS total_trips,
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
  COALESCE(at.total_trips, 0) AS total_trips,
  at.last_transaction_ts,
  DATEDIFF(CURRENT_DATE(), CAST(at.last_transaction_ts AS DATE)) AS days_since_last_purchase,
  COALESCE(rt.trips_last_30d, 0) AS trips_last_30d,
  COALESCE(pt.trips_prior_30d, 0) AS trips_prior_30d,
  COALESCE(rt.spend_last_30d, 0) AS spend_last_30d,
  COALESCE(pt.spend_prior_30d, 0) AS spend_prior_30d,
  -- trip_frequency_drop: pct drop in trips from prior to current 30d window
  CASE
    WHEN COALESCE(pt.trips_prior_30d, 0) > 0
    THEN ROUND((pt.trips_prior_30d - COALESCE(rt.trips_last_30d, 0)) * 100.0 / pt.trips_prior_30d, 1)
    ELSE NULL
  END AS trip_frequency_drop_pct,
  -- churn_risk_score: composite based on days inactive + trip drop
  CASE
    WHEN COALESCE(at.total_trips, 0) = 0 THEN 'HIGH'
    WHEN DATEDIFF(CURRENT_DATE(), CAST(at.last_transaction_ts AS DATE)) > 45 THEN 'HIGH'
    WHEN COALESCE(pt.trips_prior_30d, 0) > 0
     AND COALESCE(rt.trips_last_30d, 0) < pt.trips_prior_30d * 0.5 THEN 'HIGH'
    WHEN DATEDIFF(CURRENT_DATE(), CAST(at.last_transaction_ts AS DATE)) > 21 THEN 'MEDIUM'
    WHEN COALESCE(pt.trips_prior_30d, 0) > 0
     AND COALESCE(rt.trips_last_30d, 0) < pt.trips_prior_30d * 0.75 THEN 'MEDIUM'
    ELSE 'LOW'
  END AS churn_risk_score
FROM v2_raw.customers.household_profiles hp
LEFT JOIN all_trips at ON hp.household_id = at.household_id
LEFT JOIN recent_trips rt ON hp.household_id = rt.household_id
LEFT JOIN prior_trips pt ON hp.household_id = pt.household_id
WHERE hp.is_active = TRUE
  AND (
    COALESCE(at.total_trips, 0) = 0
    OR DATEDIFF(CURRENT_DATE(), CAST(at.last_transaction_ts AS DATE)) > 21
    OR (COALESCE(pt.trips_prior_30d, 0) > 0
        AND COALESCE(rt.trips_last_30d, 0) < pt.trips_prior_30d * 0.75)
  )
""")
print("View 7 created: v2_ontology.abstractions.atrisk_customer_radar")

# COMMAND ----------

# MAGIC %md
# MAGIC ## View 8: kpm_audiences
# MAGIC Key Performance Metric audiences — KetoDieter persona households with spend metrics
# MAGIC and lifecycle stage. PDF spec Step 4 in the Keto flow.

# COMMAND ----------

spark.sql("""
CREATE OR REPLACE VIEW v2_ontology.abstractions.kpm_audiences AS
WITH hh_keto AS (
  SELECT
    t.household_id,
    COUNT(DISTINCT CAST(t.transaction_ts AS DATE)) AS keto_purchase_days,
    SUM(CASE WHEN c.is_keto_compliant = TRUE THEN t.quantity * t.unit_price ELSE 0 END)
      AS keto_spend,
    SUM(t.quantity * t.unit_price) AS total_spend,
    SUM(CASE WHEN c.is_keto_compliant = TRUE THEN t.quantity * t.unit_price ELSE 0 END)
      / NULLIF(SUM(t.quantity * t.unit_price), 0) AS keto_basket_share,
    COUNT(DISTINCT CASE WHEN c.is_keto_compliant = TRUE THEN t.upc END)
      AS keto_sku_count
  FROM v2_raw.transactions.pos_transactions t
  JOIN v2_ontology.bridge.sku_classifications c ON t.upc = c.upc
  WHERE t.household_id IS NOT NULL
  GROUP BY t.household_id
),
recent_trips AS (
  SELECT
    household_id,
    COUNT(*) AS trips_last_30d
  FROM v2_raw.transactions.pos_transactions
  WHERE household_id IS NOT NULL
    AND transaction_ts >= CURRENT_TIMESTAMP() - INTERVAL 30 DAYS
  GROUP BY household_id
)
SELECT
  hp.household_id,
  hp.loyalty_tier,
  hp.store_id_primary,
  hp.age_band,
  hp.has_pharmacy_rx,
  COALESCE(hk.keto_basket_share, 0.0) AS keto_basket_share,
  COALESCE(hk.keto_spend, 0.0) AS keto_spend,
  COALESCE(hk.total_spend, 0.0) AS total_spend,
  COALESCE(hk.keto_sku_count, 0) AS keto_sku_count,
  -- primary_persona: KetoDieter requires keto_basket_share > 0.35 (PDF spec threshold)
  CASE
    WHEN COALESCE(hk.keto_basket_share, 0) > 0.35 THEN 'KetoDieter'
    WHEN COALESCE(hk.keto_basket_share, 0) > 0.15 THEN 'KetoInterested'
    ELSE 'General'
  END AS primary_persona,
  -- lifecycle_stage: based on recency and frequency
  CASE
    WHEN COALESCE(rt.trips_last_30d, 0) >= 4 AND COALESCE(hk.keto_basket_share, 0) > 0.35
      THEN 'ActiveKetoDieter'
    WHEN COALESCE(rt.trips_last_30d, 0) >= 2 AND COALESCE(hk.keto_basket_share, 0) > 0.35
      THEN 'CasualKetoDieter'
    WHEN COALESCE(hk.keto_basket_share, 0) > 0.35
      THEN 'LapsedKetoDieter'
    ELSE 'NonKeto'
  END AS lifecycle_stage,
  COALESCE(rt.trips_last_30d, 0) AS trips_last_30d
FROM v2_raw.customers.household_profiles hp
LEFT JOIN hh_keto hk ON hp.household_id = hk.household_id
LEFT JOIN recent_trips rt ON hp.household_id = rt.household_id
WHERE hp.is_active = TRUE
  AND COALESCE(hk.keto_basket_share, 0) > 0.35
""")
print("View 8 created: v2_ontology.abstractions.kpm_audiences")

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
        description="Customers with trip-frequency drop (current 30d vs prior 30d) — churn risk",
        source_tables="v2_raw.customers.household_profiles, v2_raw.transactions.pos_transactions",
        created_ts=datetime.utcnow(), created_by="ontology_bootstrap", is_active=True),
    Row(view_name="kpm_audiences", schema_name="v2_ontology.abstractions",
        description="KPM persona audiences — KetoDieter (keto_basket_share>0.35) with lifecycle stage",
        source_tables="v2_raw.customers.household_profiles, v2_raw.transactions.pos_transactions, v2_ontology.bridge.sku_classifications",
        created_ts=datetime.utcnow(), created_by="ontology_bootstrap", is_active=True),
]

spark.createDataFrame(view_registry_data) \
    .write.mode("overwrite") \
    .saveAsTable("v2_ontology.abstractions.view_registry")

print("View registry created with 8 entries")

# COMMAND ----------

# Verify all views
print("\n=== Verification ===")
views = [
    "unified_inventory", "dietary_sku_catalog", "substitution_graph",
    "retail_media_audiences", "margin_aware_substitution",
    "pharmacy_grocery_crosssell", "atrisk_customer_radar", "kpm_audiences"
]
for v in views:
    try:
        count = spark.table(f"v2_ontology.abstractions.{v}").count()
        print(f"  {v}: {count:,} rows")
    except Exception as e:
        print(f"  {v}: ERROR - {e}")

print("\nView registry:")
spark.table("v2_ontology.abstractions.view_registry").show(truncate=False)
