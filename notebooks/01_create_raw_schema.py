# Databricks notebook source
# TITLE: 01_create_raw_schema.py
# Create catalogs and schemas with all tables for v2_raw, v2_features, v2_ontology, v2_sharing

# COMMAND ----------

# CDF enabled per-table via TBLPROPERTIES below (SET not available on serverless)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Create Catalogs

# COMMAND ----------

# Create catalogs with explicit MANAGED LOCATION (required — this metastore has no root storage)
BASE = "abfss://nsp-test-data@stnsphv9452.dfs.core.windows.net/ontology"

for catalog, path in [
    ("v2_raw",      f"{BASE}/v2_raw"),
    ("v2_ontology", f"{BASE}/v2_ontology"),
    ("v2_features", f"{BASE}/v2_features"),
    ("v2_sharing",  f"{BASE}/v2_sharing"),
]:
    spark.sql(f"CREATE CATALOG IF NOT EXISTS {catalog} MANAGED LOCATION '{path}'")
    print(f"  Catalog created: {catalog} -> {path}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Create v2_raw schemas

# COMMAND ----------

spark.sql("CREATE SCHEMA IF NOT EXISTS v2_raw.transactions")
spark.sql("CREATE SCHEMA IF NOT EXISTS v2_raw.products")
spark.sql("CREATE SCHEMA IF NOT EXISTS v2_raw.customers")
spark.sql("CREATE SCHEMA IF NOT EXISTS v2_raw.inventory")
spark.sql("CREATE SCHEMA IF NOT EXISTS v2_raw.pharmacy")
print("v2_raw schemas created: transactions, products, customers, inventory, pharmacy")

# COMMAND ----------

# MAGIC %md
# MAGIC ## pos_transactions

# COMMAND ----------

spark.sql("""
CREATE TABLE IF NOT EXISTS v2_raw.transactions.pos_transactions (
  transaction_id        STRING        NOT NULL,
  household_id          STRING,
  store_id              STRING        NOT NULL,
  upc                   STRING        NOT NULL,
  quantity              INT           NOT NULL,
  unit_price            DECIMAL(10,2) NOT NULL,
  unit_cost             DECIMAL(10,4),
  discount_amt          DECIMAL(10,2) DEFAULT 0,
  transaction_ts        TIMESTAMP     NOT NULL,
  channel               STRING        NOT NULL,
  fulfillment_node_id   STRING,
  tender_type           STRING,
  transaction_date      DATE GENERATED ALWAYS AS (CAST(transaction_ts AS DATE))
)
USING DELTA
PARTITIONED BY (transaction_date)
TBLPROPERTIES (
  'delta.enableChangeDataFeed' = 'true',
  'delta.feature.allowColumnDefaults' = 'supported',
  'delta.autoOptimize.optimizeWrite' = 'true',
  'delta.autoOptimize.autoCompact' = 'true'
)
COMMENT 'POS and digital transactions. Partitioned by date.'
""")
print("Created v2_raw.transactions.pos_transactions")

# COMMAND ----------

# MAGIC %md
# MAGIC ## sku_master

# COMMAND ----------

spark.sql("""
CREATE TABLE IF NOT EXISTS v2_raw.products.sku_master (
  upc                   STRING        NOT NULL,
  product_name          STRING        NOT NULL,
  brand                 STRING,
  manufacturer          STRING,
  department_code       STRING,
  department_name       STRING,
  category_code         STRING,
  category_name         STRING,
  subcategory_code      STRING,
  subcategory_name      STRING,
  unit_of_measure       STRING,
  package_size          STRING,
  unit_weight_oz        DECIMAL(8,2),
  shelf_life_days       INT,
  ingredients_text      STRING,
  allergens             STRING,
  certifications        STRING,
  is_private_label      BOOLEAN DEFAULT FALSE,
  is_perishable         BOOLEAN DEFAULT FALSE,
  avg_retail_price      DECIMAL(10,2),
  effective_date        DATE,
  CONSTRAINT sku_pk PRIMARY KEY (upc)
)
USING DELTA
TBLPROPERTIES (
  'delta.enableChangeDataFeed' = 'true',
  'delta.feature.allowColumnDefaults' = 'supported'
)
COMMENT 'SKU master catalog. Facts only, no meaning embedded.'
""")
print("Created v2_raw.products.sku_master")

# COMMAND ----------

# MAGIC %md
# MAGIC ## node_inventory_snapshots

# COMMAND ----------

spark.sql("""
CREATE TABLE IF NOT EXISTS v2_raw.inventory.node_inventory_snapshots (
  snapshot_id           STRING        NOT NULL,
  node_id               STRING        NOT NULL,
  node_type             STRING        NOT NULL,
  upc                   STRING        NOT NULL,
  on_hand_qty           INT           NOT NULL,
  reserved_qty          INT DEFAULT 0,
  in_transit_qty        INT DEFAULT 0,
  reorder_point         INT,
  snapshot_ts           TIMESTAMP     NOT NULL,
  snapshot_date         DATE GENERATED ALWAYS AS (CAST(snapshot_ts AS DATE))
)
USING DELTA
PARTITIONED BY (snapshot_date)
TBLPROPERTIES (
  'delta.enableChangeDataFeed' = 'true',
  'delta.feature.allowColumnDefaults' = 'supported'
)
COMMENT 'Hourly inventory snapshots across all fulfillment nodes.'
""")
print("Created v2_raw.inventory.node_inventory_snapshots")

# COMMAND ----------

# MAGIC %md
# MAGIC ## household_profiles

# COMMAND ----------

spark.sql("""
CREATE TABLE IF NOT EXISTS v2_raw.customers.household_profiles (
  household_id          STRING        NOT NULL,
  zip_code              STRING,
  store_id_primary      STRING,
  loyalty_tier          STRING,
  household_size        INT,
  enrollment_date       DATE,
  is_active             BOOLEAN,
  age_band              STRING,
  has_pharmacy_rx       BOOLEAN DEFAULT FALSE,
  has_fuel_rewards      BOOLEAN DEFAULT FALSE,
  CONSTRAINT hh_pk PRIMARY KEY (household_id)
)
USING DELTA
TBLPROPERTIES (
  'delta.enableChangeDataFeed' = 'true',
  'delta.feature.allowColumnDefaults' = 'supported'
)
COMMENT 'Household profiles with loyalty and demographic data.'
""")
print("Created v2_raw.customers.household_profiles")

# COMMAND ----------

# MAGIC %md
# MAGIC ## prescriptions

# COMMAND ----------

spark.sql("""
CREATE TABLE IF NOT EXISTS v2_raw.pharmacy.prescriptions (
  rx_id                 STRING        NOT NULL,
  household_id          STRING,
  store_id              STRING,
  ndc_code              STRING,
  drug_name             STRING,
  drug_class            STRING,
  fill_date             DATE,
  days_supply           INT,
  quantity              DECIMAL(10,2),
  refill_number         INT,
  prescriber_id         STRING,
  copay_amount          DECIMAL(10,2),
  CONSTRAINT rx_pk PRIMARY KEY (rx_id)
)
USING DELTA
TBLPROPERTIES ('delta.enableChangeDataFeed' = 'true')
COMMENT 'Pharmacy prescription records.'
""")
print("Created v2_raw.pharmacy.prescriptions")

# COMMAND ----------

# MAGIC %md
# MAGIC ## drug_interactions

# COMMAND ----------

spark.sql("""
CREATE TABLE IF NOT EXISTS v2_raw.pharmacy.drug_interactions (
  drug_class_1            STRING,
  drug_class_2            STRING,
  interaction_severity    STRING,
  interaction_description STRING,
  food_interaction_flag   BOOLEAN,
  food_interaction_detail STRING
)
USING DELTA
COMMENT 'Drug-drug and drug-food interaction reference data.'
""")
print("Created v2_raw.pharmacy.drug_interactions")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Create v2_features schemas

# COMMAND ----------

spark.sql("CREATE SCHEMA IF NOT EXISTS v2_features.product_features")
spark.sql("CREATE SCHEMA IF NOT EXISTS v2_features.customer_features")
print("v2_features schemas created: product_features, customer_features")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Create v2_ontology catalog (schemas created in 04a)

# COMMAND ----------

# v2_ontology catalog already created above
print("v2_ontology catalog ready (schemas will be created in 04a)")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Create v2_sharing schema

# COMMAND ----------

spark.sql("CREATE SCHEMA IF NOT EXISTS v2_sharing.cpg_exports")
print("v2_sharing schema created: cpg_exports")

# COMMAND ----------

print("\n=== Schema creation complete ===")
print("v2_raw:      transactions, products, customers, inventory, pharmacy")
print("v2_features: product_features, customer_features")
print("v2_ontology: (schemas in 04a)")
print("v2_sharing:  cpg_exports")
