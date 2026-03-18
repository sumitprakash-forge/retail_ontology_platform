# Databricks notebook source
# MAGIC %md
# MAGIC # 10 - Create Unity Catalog Metric Views
# MAGIC Defines business metric views in v2_ontology.metrics for consistent
# MAGIC metric definitions queryable by Genie and SQL.

# COMMAND ----------

spark.sql("CREATE SCHEMA IF NOT EXISTS v2_ontology.metrics")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Metric View 1: daily_sales_performance

# COMMAND ----------

spark.sql("""
CREATE OR REPLACE VIEW v2_ontology.metrics.daily_sales_performance AS
SELECT
  CAST(transaction_ts AS DATE) AS transaction_date,
  COUNT(*) AS transaction_count,
  SUM(quantity * unit_price) AS total_revenue,
  SUM(quantity * unit_price) / NULLIF(COUNT(DISTINCT transaction_id), 0) AS avg_basket_value,
  COUNT(DISTINCT household_id) AS unique_households
FROM v2_raw.transactions.pos_transactions
GROUP BY CAST(transaction_ts AS DATE)
""")
print("Metric view 1 created: daily_sales_performance")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Metric View 2: inventory_health

# COMMAND ----------

spark.sql("""
CREATE OR REPLACE VIEW v2_ontology.metrics.inventory_health AS
SELECT
  node_id,
  node_type,
  upc,
  on_hand_qty,
  reorder_point,
  on_hand_qty / NULLIF(reorder_point, 0) AS days_of_supply,
  CASE WHEN on_hand_qty < reorder_point THEN TRUE ELSE FALSE END AS is_below_reorder
FROM v2_raw.inventory.node_inventory_snapshots
""")
print("Metric view 2 created: inventory_health")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Metric View 3: customer_lifecycle

# COMMAND ----------

spark.sql("""
CREATE OR REPLACE VIEW v2_ontology.metrics.customer_lifecycle AS
WITH txn_summary AS (
  SELECT
    household_id,
    COUNT(*) AS total_transactions,
    SUM(quantity * unit_price) AS total_spend,
    MAX(transaction_ts) AS last_transaction_date
  FROM v2_raw.transactions.pos_transactions
  WHERE household_id IS NOT NULL
  GROUP BY household_id
)
SELECT
  hp.household_id,
  hp.loyalty_tier,
  hp.enrollment_date,
  hp.is_active,
  COALESCE(ts.total_transactions, 0) AS total_transactions,
  COALESCE(ts.total_spend, 0) AS total_spend,
  ts.last_transaction_date,
  DATEDIFF(CURRENT_DATE(), CAST(ts.last_transaction_date AS DATE)) AS days_since_last_purchase
FROM v2_raw.customers.household_profiles hp
LEFT JOIN txn_summary ts ON hp.household_id = ts.household_id
""")
print("Metric view 3 created: customer_lifecycle")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Metric View 4: dietary_cohort_performance

# COMMAND ----------

spark.sql("""
CREATE OR REPLACE VIEW v2_ontology.metrics.dietary_cohort_performance AS
SELECT
  'keto' AS dietary_cohort,
  SUM(CASE WHEN c.is_keto_compliant = TRUE THEN t.quantity * t.unit_price ELSE 0 END) AS cohort_revenue,
  COUNT(DISTINCT CASE WHEN c.is_keto_compliant = TRUE THEN t.transaction_id END) AS cohort_transactions,
  COUNT(DISTINCT CASE WHEN c.is_keto_compliant = TRUE THEN t.household_id END) AS cohort_households
FROM v2_ontology.bridge.sku_classifications c
JOIN v2_raw.transactions.pos_transactions t ON c.upc = t.upc

UNION ALL

SELECT
  'vegan' AS dietary_cohort,
  SUM(CASE WHEN c.is_vegan = TRUE THEN t.quantity * t.unit_price ELSE 0 END) AS cohort_revenue,
  COUNT(DISTINCT CASE WHEN c.is_vegan = TRUE THEN t.transaction_id END) AS cohort_transactions,
  COUNT(DISTINCT CASE WHEN c.is_vegan = TRUE THEN t.household_id END) AS cohort_households
FROM v2_ontology.bridge.sku_classifications c
JOIN v2_raw.transactions.pos_transactions t ON c.upc = t.upc

UNION ALL

SELECT
  'gluten_free' AS dietary_cohort,
  SUM(CASE WHEN c.is_gluten_free = TRUE THEN t.quantity * t.unit_price ELSE 0 END) AS cohort_revenue,
  COUNT(DISTINCT CASE WHEN c.is_gluten_free = TRUE THEN t.transaction_id END) AS cohort_transactions,
  COUNT(DISTINCT CASE WHEN c.is_gluten_free = TRUE THEN t.household_id END) AS cohort_households
FROM v2_ontology.bridge.sku_classifications c
JOIN v2_raw.transactions.pos_transactions t ON c.upc = t.upc

UNION ALL

SELECT
  'organic' AS dietary_cohort,
  SUM(CASE WHEN c.is_organic = TRUE THEN t.quantity * t.unit_price ELSE 0 END) AS cohort_revenue,
  COUNT(DISTINCT CASE WHEN c.is_organic = TRUE THEN t.transaction_id END) AS cohort_transactions,
  COUNT(DISTINCT CASE WHEN c.is_organic = TRUE THEN t.household_id END) AS cohort_households
FROM v2_ontology.bridge.sku_classifications c
JOIN v2_raw.transactions.pos_transactions t ON c.upc = t.upc

UNION ALL

SELECT
  'dairy_free' AS dietary_cohort,
  SUM(CASE WHEN c.is_dairy_free = TRUE THEN t.quantity * t.unit_price ELSE 0 END) AS cohort_revenue,
  COUNT(DISTINCT CASE WHEN c.is_dairy_free = TRUE THEN t.transaction_id END) AS cohort_transactions,
  COUNT(DISTINCT CASE WHEN c.is_dairy_free = TRUE THEN t.household_id END) AS cohort_households
FROM v2_ontology.bridge.sku_classifications c
JOIN v2_raw.transactions.pos_transactions t ON c.upc = t.upc

UNION ALL

SELECT
  'paleo' AS dietary_cohort,
  SUM(CASE WHEN c.is_paleo = TRUE THEN t.quantity * t.unit_price ELSE 0 END) AS cohort_revenue,
  COUNT(DISTINCT CASE WHEN c.is_paleo = TRUE THEN t.transaction_id END) AS cohort_transactions,
  COUNT(DISTINCT CASE WHEN c.is_paleo = TRUE THEN t.household_id END) AS cohort_households
FROM v2_ontology.bridge.sku_classifications c
JOIN v2_raw.transactions.pos_transactions t ON c.upc = t.upc

UNION ALL

SELECT
  'plant_based' AS dietary_cohort,
  SUM(CASE WHEN c.is_plant_based = TRUE THEN t.quantity * t.unit_price ELSE 0 END) AS cohort_revenue,
  COUNT(DISTINCT CASE WHEN c.is_plant_based = TRUE THEN t.transaction_id END) AS cohort_transactions,
  COUNT(DISTINCT CASE WHEN c.is_plant_based = TRUE THEN t.household_id END) AS cohort_households
FROM v2_ontology.bridge.sku_classifications c
JOIN v2_raw.transactions.pos_transactions t ON c.upc = t.upc
""")
print("Metric view 4 created: dietary_cohort_performance")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Metric View 5: substitution_impact

# COMMAND ----------

spark.sql("""
CREATE OR REPLACE VIEW v2_ontology.metrics.substitution_impact AS
SELECT
  shared_class AS dietary_class,
  COUNT(*) AS substitution_pair_count,
  AVG(weight) AS avg_similarity_score
FROM (
  SELECT
    r.weight,
    EXPLODE(r.shared_class_ids) AS shared_class
  FROM v2_ontology.graph.relationships r
  WHERE r.rel_type = 'SUBSTITUTABLE_FOR'
)
GROUP BY shared_class
""")
print("Metric view 5 created: substitution_impact")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Metric View 6: pharmacy_crosssell_metrics

# COMMAND ----------

spark.sql("""
CREATE OR REPLACE VIEW v2_ontology.metrics.pharmacy_crosssell_metrics AS
SELECT
  drug_class,
  COUNT(DISTINCT household_id) AS household_count,
  COUNT(*) AS food_interaction_count
FROM v2_ontology.abstractions.pharmacy_grocery_crosssell
GROUP BY drug_class
""")
print("Metric view 6 created: pharmacy_crosssell_metrics")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Metric View 7: ontology_coverage

# COMMAND ----------

spark.sql("""
CREATE OR REPLACE VIEW v2_ontology.metrics.ontology_coverage AS
SELECT 'tier2_product' AS domain, COUNT(*) AS class_count FROM v2_ontology.tier2_product.classes
UNION ALL
SELECT 'tier2_customer' AS domain, COUNT(*) AS class_count FROM v2_ontology.tier2_customer.classes
UNION ALL
SELECT 'tier2_supply_chain' AS domain, COUNT(*) AS class_count FROM v2_ontology.tier2_supply_chain.classes
UNION ALL
SELECT 'tier2_store_ops' AS domain, COUNT(*) AS class_count FROM v2_ontology.tier2_store_ops.classes
UNION ALL
SELECT 'tier2_pharmacy' AS domain, COUNT(*) AS class_count FROM v2_ontology.tier2_pharmacy.classes
UNION ALL
SELECT 'tier2_finance' AS domain, COUNT(*) AS class_count FROM v2_ontology.tier2_finance.classes
UNION ALL
SELECT 'tier2_media' AS domain, COUNT(*) AS class_count FROM v2_ontology.tier2_media.classes
""")
print("Metric view 7 created: ontology_coverage")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Create validation_results Table (Delta)

# COMMAND ----------

spark.sql("""
CREATE TABLE IF NOT EXISTS v2_ontology.metrics.validation_results (
  test_id STRING NOT NULL,
  test_phase STRING NOT NULL,
  test_name STRING NOT NULL,
  status STRING NOT NULL,
  message STRING,
  details STRING,
  run_ts TIMESTAMP NOT NULL,
  duration_seconds DOUBLE
)
USING DELTA
TBLPROPERTIES ('delta.enableChangeDataFeed' = 'true')
COMMENT 'Stores e2e validation test results from 15_e2e_validation'
""")
print("validation_results Delta table created")

# COMMAND ----------

# Verify all metric views
print("\n=== Metric Views Verification ===")
metric_views = [
    "daily_sales_performance", "inventory_health", "customer_lifecycle",
    "dietary_cohort_performance", "substitution_impact",
    "pharmacy_crosssell_metrics", "ontology_coverage"
]
for v in metric_views:
    try:
        count = spark.table(f"v2_ontology.metrics.{v}").count()
        print(f"  {v}: {count:,} rows")
    except Exception as e:
        print(f"  {v}: ERROR - {e}")
