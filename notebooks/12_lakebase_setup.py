# Databricks notebook source
# MAGIC %md
# MAGIC # 12 - Lakebase / Agent Memory Setup
# MAGIC Sets up agent memory tables. Tries Lakebase first, falls back to Delta tables
# MAGIC in v2_ontology.graph.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Attempt Lakebase Setup

# COMMAND ----------

lakebase_available = False

try:
    from databricks.sdk import WorkspaceClient
    w = WorkspaceClient()

    print("Attempting Lakebase database creation...")
    instance = w.database.create(
        name="ontology-agent-memory",
        catalog="v2_ontology",
        schema="graph"
    )
    print(f"Lakebase instance created: {instance}")
    lakebase_available = True
except ImportError:
    print("Databricks SDK not available. Falling back to Delta tables.")
except AttributeError:
    print("Lakebase API (w.database.create) not available in this SDK version. Falling back to Delta tables.")
except Exception as e:
    print(f"Lakebase creation failed: {e}")
    print("Falling back to Delta tables.")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Fallback: Create Delta Tables in v2_ontology.graph

# COMMAND ----------

if not lakebase_available:
    print("Creating agent memory tables as Delta tables in v2_ontology.graph...")

    # Ensure schema exists
    spark.sql("CREATE SCHEMA IF NOT EXISTS v2_ontology.graph")

    # agent_sessions table
    spark.sql("""
    CREATE TABLE IF NOT EXISTS v2_ontology.graph.agent_sessions (
      session_id STRING NOT NULL,
      household_id STRING,
      started_ts TIMESTAMP,
      ended_ts TIMESTAMP,
      agent_version STRING,
      session_metadata STRING,
      CONSTRAINT agent_sessions_pk PRIMARY KEY (session_id)
    )
    USING DELTA
    TBLPROPERTIES ('delta.enableChangeDataFeed' = 'true')
    COMMENT 'Agent session tracking for ontology shopping assistant'
    """)
    print("  Created: v2_ontology.graph.agent_sessions")

    # agent_memory table
    spark.sql("""
    CREATE TABLE IF NOT EXISTS v2_ontology.graph.agent_memory (
      memory_id STRING NOT NULL,
      session_id STRING,
      household_id STRING,
      memory_type STRING,
      memory_key STRING,
      memory_value STRING,
      created_ts TIMESTAMP,
      expires_ts TIMESTAMP,
      CONSTRAINT agent_memory_pk PRIMARY KEY (memory_id)
    )
    USING DELTA
    TBLPROPERTIES ('delta.enableChangeDataFeed' = 'true')
    COMMENT 'Agent memory store for conversation context and preferences'
    """)
    print("  Created: v2_ontology.graph.agent_memory")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Bridge Table: household_lifecycle
# MAGIC Computed from transactions — lifecycle stage per household. PDF spec bridge.

# COMMAND ----------

spark.sql("""
CREATE OR REPLACE TABLE v2_ontology.graph.household_lifecycle (
  household_id        STRING NOT NULL,
  lifecycle_stage     STRING,
  primary_persona     STRING,
  keto_basket_share   DOUBLE,
  total_spend_90d     DOUBLE,
  trips_90d           LONG,
  trips_last_30d      LONG,
  trips_prior_30d     LONG,
  trip_frequency_drop_pct DOUBLE,
  last_transaction_ts TIMESTAMP,
  computed_ts         TIMESTAMP,
  CONSTRAINT hh_lifecycle_pk PRIMARY KEY (household_id)
)
USING DELTA
TBLPROPERTIES ('delta.enableChangeDataFeed' = 'true')
COMMENT 'Household lifecycle stage computed from transaction history. Refreshed daily by Lakeflow.'
""")
print("  Schema created: v2_ontology.graph.household_lifecycle")

# Compute and populate from transactions
try:
    spark.sql("""
    INSERT OVERWRITE v2_ontology.graph.household_lifecycle
    WITH base AS (
      SELECT
        t.household_id,
        SUM(t.quantity * t.unit_price) AS total_spend_90d,
        COUNT(*) AS trips_90d,
        SUM(CASE WHEN c.is_keto_compliant = TRUE THEN t.quantity * t.unit_price ELSE 0 END)
          AS keto_spend,
        SUM(CASE WHEN t.transaction_ts >= CURRENT_TIMESTAMP() - INTERVAL 30 DAYS THEN 1 ELSE 0 END)
          AS trips_last_30d,
        SUM(CASE WHEN t.transaction_ts >= CURRENT_TIMESTAMP() - INTERVAL 60 DAYS
                  AND t.transaction_ts <  CURRENT_TIMESTAMP() - INTERVAL 30 DAYS THEN 1 ELSE 0 END)
          AS trips_prior_30d,
        MAX(t.transaction_ts) AS last_transaction_ts
      FROM v2_raw.transactions.pos_transactions t
      LEFT JOIN v2_ontology.bridge.sku_classifications c ON t.upc = c.upc
      WHERE t.household_id IS NOT NULL
        AND t.transaction_ts >= CURRENT_TIMESTAMP() - INTERVAL 90 DAYS
      GROUP BY t.household_id
    )
    SELECT
      household_id,
      CASE
        WHEN keto_spend / NULLIF(total_spend_90d, 0) > 0.35
          AND trips_last_30d >= 4 THEN 'ActiveKetoDieter'
        WHEN keto_spend / NULLIF(total_spend_90d, 0) > 0.35
          AND trips_last_30d >= 2 THEN 'CasualKetoDieter'
        WHEN keto_spend / NULLIF(total_spend_90d, 0) > 0.35
          THEN 'LapsedKetoDieter'
        WHEN keto_spend / NULLIF(total_spend_90d, 0) > 0.15 THEN 'KetoInterested'
        WHEN trips_prior_30d > 0
          AND trips_last_30d < trips_prior_30d * 0.5 THEN 'AtRisk'
        WHEN trips_90d >= 8 THEN 'HighFrequency'
        WHEN trips_90d >= 4 THEN 'Regular'
        ELSE 'Occasional'
      END AS lifecycle_stage,
      CASE
        WHEN keto_spend / NULLIF(total_spend_90d, 0) > 0.35 THEN 'KetoDieter'
        WHEN keto_spend / NULLIF(total_spend_90d, 0) > 0.15 THEN 'KetoInterested'
        ELSE 'General'
      END AS primary_persona,
      ROUND(keto_spend / NULLIF(total_spend_90d, 0), 4) AS keto_basket_share,
      ROUND(total_spend_90d, 2) AS total_spend_90d,
      trips_90d,
      trips_last_30d,
      trips_prior_30d,
      CASE
        WHEN trips_prior_30d > 0
        THEN ROUND((trips_prior_30d - trips_last_30d) * 100.0 / trips_prior_30d, 1)
        ELSE NULL
      END AS trip_frequency_drop_pct,
      last_transaction_ts,
      CURRENT_TIMESTAMP() AS computed_ts
    FROM base
    """)
    count = spark.table("v2_ontology.graph.household_lifecycle").count()
    print(f"  household_lifecycle populated: {count:,} rows")
except Exception as e:
    print(f"  household_lifecycle population error: {e}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Bridge Table: rx_therapeutic_class
# MAGIC Derived from rx_claims — therapeutic class per household. PDF spec bridge.

# COMMAND ----------

spark.sql("""
CREATE OR REPLACE TABLE v2_ontology.graph.rx_therapeutic_class (
  household_id          STRING NOT NULL,
  drug_class            STRING NOT NULL,
  therapeutic_category  STRING,
  rx_count              LONG,
  active_rx_count       LONG,
  food_interaction_flag BOOLEAN,
  dietary_contraindications STRING,
  computed_ts           TIMESTAMP,
  CONSTRAINT rx_tc_pk PRIMARY KEY (household_id, drug_class)
)
USING DELTA
TBLPROPERTIES ('delta.enableChangeDataFeed' = 'true')
COMMENT 'Therapeutic class per household derived from prescriptions. Used for pharmacy cross-sell.'
""")
print("  Schema created: v2_ontology.graph.rx_therapeutic_class")

try:
    spark.sql("""
    INSERT OVERWRITE v2_ontology.graph.rx_therapeutic_class
    SELECT
      rx.household_id,
      rx.drug_class,
      CASE
        WHEN rx.drug_class LIKE '%STATIN%' OR rx.drug_class LIKE '%CHOLESTEROL%' THEN 'Cardiovascular'
        WHEN rx.drug_class LIKE '%INSULIN%' OR rx.drug_class LIKE '%DIABETES%' THEN 'Endocrine'
        WHEN rx.drug_class LIKE '%ANTIDEPRESSANT%' OR rx.drug_class LIKE '%SSRI%' THEN 'Mental Health'
        WHEN rx.drug_class LIKE '%ANTIBIOTIC%' OR rx.drug_class LIKE '%PENICILLIN%' THEN 'Infectious Disease'
        WHEN rx.drug_class LIKE '%NSAID%' OR rx.drug_class LIKE '%ANALGESIC%' THEN 'Pain Management'
        ELSE 'Other'
      END AS therapeutic_category,
      COUNT(*) AS rx_count,
      SUM(CASE WHEN rx.days_supply > 0 THEN 1 ELSE 0 END) AS active_rx_count,
      COALESCE(MAX(CAST(di.food_interaction_flag AS BOOLEAN)), FALSE) AS food_interaction_flag,
      FIRST(di.food_interaction_detail, TRUE) AS dietary_contraindications,
      CURRENT_TIMESTAMP() AS computed_ts
    FROM v2_raw.pharmacy.prescriptions rx
    LEFT JOIN v2_raw.pharmacy.drug_interactions di ON rx.drug_class = di.drug_class_1
    WHERE rx.household_id IS NOT NULL
    GROUP BY rx.household_id, rx.drug_class
    """)
    count = spark.table("v2_ontology.graph.rx_therapeutic_class").count()
    print(f"  rx_therapeutic_class populated: {count:,} rows")
except Exception as e:
    print(f"  rx_therapeutic_class population error: {e}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Bridge Table: sku_gs1_mapping
# MAGIC Derived from sku_master dept/category codes — GS1 segment/family/class mapping.

# COMMAND ----------

spark.sql("""
CREATE OR REPLACE TABLE v2_ontology.graph.sku_gs1_mapping (
  upc               STRING NOT NULL,
  gs1_segment       STRING,
  gs1_family        STRING,
  gs1_class         STRING,
  gs1_brick         STRING,
  department_code   STRING,
  department_name   STRING,
  category_code     STRING,
  category_name     STRING,
  computed_ts       TIMESTAMP,
  CONSTRAINT sku_gs1_pk PRIMARY KEY (upc)
)
USING DELTA
TBLPROPERTIES ('delta.enableChangeDataFeed' = 'true')
COMMENT 'GS1 segment/family/class mapping derived from sku_master department and category codes.'
""")
print("  Schema created: v2_ontology.graph.sku_gs1_mapping")

try:
    spark.sql("""
    INSERT OVERWRITE v2_ontology.graph.sku_gs1_mapping
    SELECT
      upc,
      -- gs1_segment: top-level GS1 hierarchy derived from department
      CASE
        WHEN department_name LIKE '%GROCERY%' OR department_name LIKE '%FOOD%' THEN 'Food/Beverage/Tobacco'
        WHEN department_name LIKE '%PHARMACY%' OR department_name LIKE '%HEALTH%' THEN 'Health/Beauty/Personal Care'
        WHEN department_name LIKE '%MEAT%' OR department_name LIKE '%SEAFOOD%' THEN 'Food/Beverage/Tobacco'
        WHEN department_name LIKE '%PRODUCE%' OR department_name LIKE '%BAKERY%' THEN 'Food/Beverage/Tobacco'
        WHEN department_name LIKE '%DAIRY%' OR department_name LIKE '%DELI%' THEN 'Food/Beverage/Tobacco'
        WHEN department_name LIKE '%FROZEN%' THEN 'Food/Beverage/Tobacco'
        WHEN department_name LIKE '%HOUSEHOLD%' OR department_name LIKE '%CLEANING%' THEN 'Household/Office/Garden'
        ELSE 'General Merchandise'
      END AS gs1_segment,
      -- gs1_family derived from department_name
      COALESCE(department_name, 'Unknown') AS gs1_family,
      -- gs1_class derived from category_name
      COALESCE(category_name, 'Uncategorized') AS gs1_class,
      -- gs1_brick = subcategory_name if available, else category
      COALESCE(subcategory_name, category_name, 'General') AS gs1_brick,
      department_code,
      department_name,
      category_code,
      category_name,
      CURRENT_TIMESTAMP() AS computed_ts
    FROM v2_raw.products.sku_master
    """)
    count = spark.table("v2_ontology.graph.sku_gs1_mapping").count()
    print(f"  sku_gs1_mapping populated: {count:,} rows")
except Exception as e:
    print(f"  sku_gs1_mapping population error: {e}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Verify Tables

# COMMAND ----------

print("\n=== Agent Memory + Bridge Tables Verification ===")
for table_name in ["agent_sessions", "agent_memory",
                   "household_lifecycle", "rx_therapeutic_class", "sku_gs1_mapping"]:
    try:
        df = spark.table(f"v2_ontology.graph.{table_name}")
        print(f"  v2_ontology.graph.{table_name}: OK (columns: {df.columns})")
    except Exception as e:
        print(f"  v2_ontology.graph.{table_name}: ERROR - {e}")

print("\nAgent memory setup complete.")
if lakebase_available:
    print("Storage: Lakebase (Postgres-compatible)")
else:
    print("Storage: Delta Lake (v2_ontology.graph)")
