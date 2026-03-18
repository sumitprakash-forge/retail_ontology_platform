# Databricks notebook source
# MAGIC %md
# MAGIC # 03a - Create Tier 1 Foundation
# MAGIC Creates the `v2_ontology` catalog, `tier1_foundation` schema, and the top-level ontology tables.
# MAGIC CRITICAL: This is frozen after creation. Changes require Architecture Review Board sign-off.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Create schema

# COMMAND ----------

spark.sql("CREATE SCHEMA IF NOT EXISTS v2_ontology.tier1_foundation")
print("Schema v2_ontology.tier1_foundation created")

# COMMAND ----------

# MAGIC %md
# MAGIC ## classes_top

# COMMAND ----------

spark.sql("""
CREATE TABLE IF NOT EXISTS v2_ontology.tier1_foundation.classes_top (
  top_class_id    STRING  NOT NULL,
  top_class_name  STRING  NOT NULL,
  description     STRING,
  is_abstract     BOOLEAN DEFAULT TRUE,
  locked_ts       TIMESTAMP,
  CONSTRAINT top_pk PRIMARY KEY (top_class_id)
)
USING DELTA
TBLPROPERTIES ('delta.feature.allowColumnDefaults' = 'supported')
COMMENT 'FROZEN. Universal top-level ontology classes. All domain classes inherit from these.'
""")
print("Created v2_ontology.tier1_foundation.classes_top")

# COMMAND ----------

# MAGIC %md
# MAGIC ## relationship_types

# COMMAND ----------

spark.sql("""
CREATE TABLE IF NOT EXISTS v2_ontology.tier1_foundation.relationship_types (
  rel_type_code     STRING  NOT NULL,
  rel_type_name     STRING  NOT NULL,
  source_top_class  STRING  NOT NULL,
  target_top_class  STRING  NOT NULL,
  is_symmetric      BOOLEAN DEFAULT FALSE,
  is_transitive     BOOLEAN DEFAULT FALSE,
  domain_owner      STRING,
  description       STRING,
  CONSTRAINT rel_types_pk PRIMARY KEY (rel_type_code)
)
USING DELTA
TBLPROPERTIES ('delta.feature.allowColumnDefaults' = 'supported')
COMMENT 'Master registry of all valid relationship type codes. Prevents synonym proliferation.'
""")
print("Created v2_ontology.tier1_foundation.relationship_types")

# COMMAND ----------

print("\nTier 1 foundation tables created successfully.")
print("  - v2_ontology.tier1_foundation.classes_top")
print("  - v2_ontology.tier1_foundation.relationship_types")
