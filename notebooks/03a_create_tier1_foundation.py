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

# COMMAND ----------

# MAGIC %md
# MAGIC ## version_registry
# MAGIC Tracks schema version history for all Tier 1 objects. Required by PDF spec.

# COMMAND ----------

spark.sql("""
CREATE TABLE IF NOT EXISTS v2_ontology.tier1_foundation.version_registry (
  version_id      STRING  NOT NULL,
  object_type     STRING  NOT NULL,
  object_name     STRING  NOT NULL,
  version_number  STRING  NOT NULL,
  change_summary  STRING,
  approved_by     STRING,
  approved_ts     TIMESTAMP,
  is_current      BOOLEAN DEFAULT TRUE,
  created_ts      TIMESTAMP,
  CONSTRAINT version_registry_pk PRIMARY KEY (version_id)
)
USING DELTA
TBLPROPERTIES ('delta.feature.allowColumnDefaults' = 'supported')
COMMENT 'FROZEN. Version registry for all Tier 1 ontology schema changes. Requires ARB sign-off.'
""")
print("Created v2_ontology.tier1_foundation.version_registry")

# Seed initial version entries for classes_top and relationship_types
from pyspark.sql import Row
from datetime import datetime

seed_versions = [
    Row(version_id="v1-classes_top", object_type="TABLE", object_name="classes_top",
        version_number="1.0.0", change_summary="Initial creation — 7 universal top-level classes",
        approved_by="architecture_review_board", approved_ts=datetime(2024, 1, 1),
        is_current=True, created_ts=datetime.utcnow()),
    Row(version_id="v1-relationship_types", object_type="TABLE", object_name="relationship_types",
        version_number="1.0.0", change_summary="Initial creation — 5 canonical relationship type codes",
        approved_by="architecture_review_board", approved_ts=datetime(2024, 1, 1),
        is_current=True, created_ts=datetime.utcnow()),
    Row(version_id="v1-version_registry", object_type="TABLE", object_name="version_registry",
        version_number="1.0.0", change_summary="Bootstrap — version registry itself",
        approved_by="architecture_review_board", approved_ts=datetime(2024, 1, 1),
        is_current=True, created_ts=datetime.utcnow()),
]

spark.createDataFrame(seed_versions) \
    .write.mode("overwrite") \
    .saveAsTable("v2_ontology.tier1_foundation.version_registry")
print("version_registry seeded with 3 initial entries")

# COMMAND ----------

print("\nTier 1 foundation tables created successfully.")
print("  - v2_ontology.tier1_foundation.classes_top")
print("  - v2_ontology.tier1_foundation.relationship_types")
print("  - v2_ontology.tier1_foundation.version_registry")
