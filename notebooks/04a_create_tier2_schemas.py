# Databricks notebook source
# 04a - Create Tier 2 Domain Schemas & Tables
# Creates all tier2 domain schemas, domain class tables, graph tables,
# bridge tables, and governance tables in v2_ontology per spec Section 6.2.

# COMMAND ----------

# Create all schemas in v2_ontology
schemas = [
    "tier2_product",
    "tier2_customer",
    "tier2_supply_chain",
    "tier2_store_ops",
    "tier2_pharmacy",
    "tier2_finance",
    "tier2_media",
    "graph",
    "bridge",
    "abstractions",
    "metrics",
    "sharing",
]

for schema in schemas:
    spark.sql(f"CREATE SCHEMA IF NOT EXISTS v2_ontology.{schema}")
    print(f"Schema created: v2_ontology.{schema}")

# COMMAND ----------

# Create domain classes table for each of the 7 tier2 domains
# Table name is `classes` (not `domain_classes`) per spec

tier2_domains = [
    "tier2_product",
    "tier2_customer",
    "tier2_supply_chain",
    "tier2_store_ops",
    "tier2_pharmacy",
    "tier2_finance",
    "tier2_media",
]

for domain in tier2_domains:
    spark.sql(f"""
    CREATE TABLE IF NOT EXISTS v2_ontology.{domain}.classes (
      class_id          STRING  NOT NULL,
      class_name        STRING  NOT NULL,
      parent_class_id   STRING,
      parent_top_class  STRING  NOT NULL,
      class_path        STRING,
      description       STRING,
      domain            STRING  DEFAULT '{domain}',
      ontology_version  STRING  DEFAULT '1.0.0',
      is_active         BOOLEAN DEFAULT TRUE,
      is_ai_inferrable  BOOLEAN DEFAULT FALSE,
      created_by        STRING,
      created_ts        TIMESTAMP,
      updated_ts        TIMESTAMP,
      CONSTRAINT {domain}_classes_pk PRIMARY KEY (class_id)
    )
    USING DELTA
    TBLPROPERTIES (
      'delta.enableChangeDataFeed' = 'true',
      'delta.feature.allowColumnDefaults' = 'supported'
    )
    COMMENT 'Tier 2 ontology classes for {domain}'
    """)
    print(f"  Table created: v2_ontology.{domain}.classes")

# COMMAND ----------

# Create graph.relationships table
spark.sql("""
CREATE TABLE IF NOT EXISTS v2_ontology.graph.relationships (
  rel_id              STRING    NOT NULL,
  rel_type            STRING    NOT NULL,
  rel_type_code       STRING,
  source_class        STRING    NOT NULL,
  target_class        STRING    NOT NULL,
  source_entity_id    STRING,
  target_entity_id    STRING,
  domain_source       STRING,
  domain_target       STRING,
  weight              DOUBLE    DEFAULT 1.0,
  shared_class_ids    ARRAY<STRING>,
  inference_rule      STRING,
  is_inferred         BOOLEAN   DEFAULT FALSE,
  model_version       STRING,
  created_ts          TIMESTAMP,
  expires_ts          TIMESTAMP,
  CONSTRAINT rels_pk PRIMARY KEY (rel_id)
)
USING DELTA
TBLPROPERTIES (
  'delta.enableChangeDataFeed' = 'true',
  'delta.feature.allowColumnDefaults' = 'supported'
)
COMMENT 'Cross-domain semantic graph relationships'
""")
print("Table created: v2_ontology.graph.relationships")

# COMMAND ----------

# Create bridge.sku_classifications table
spark.sql("""
CREATE TABLE IF NOT EXISTS v2_ontology.bridge.sku_classifications (
  upc                     STRING    NOT NULL,
  is_keto_compliant       BOOLEAN,
  is_vegan                BOOLEAN,
  is_gluten_free          BOOLEAN,
  is_organic              BOOLEAN,
  is_dairy_free           BOOLEAN,
  is_paleo                BOOLEAN,
  is_plant_based          BOOLEAN,
  is_mediterranean        BOOLEAN,
  contains_tree_nuts      BOOLEAN,
  contains_dairy          BOOLEAN,
  contains_gluten         BOOLEAN,
  contains_shellfish      BOOLEAN,
  contains_soy            BOOLEAN,
  contains_eggs           BOOLEAN,
  margin_segment          STRING,
  is_pharmacy_product     BOOLEAN,
  therapeutic_class       STRING,
  confidence_score        DOUBLE,
  classification_notes    STRING,
  classification_ts       TIMESTAMP,
  model_used              STRING,
  CONSTRAINT sku_class_pk PRIMARY KEY (upc)
)
USING DELTA
TBLPROPERTIES ('delta.enableChangeDataFeed' = 'true')
COMMENT 'AI-classified SKU dietary, allergen, margin, and pharmacy attributes'
""")
print("Table created: v2_ontology.bridge.sku_classifications")

# COMMAND ----------

# Create graph.ontology_versions governance table
spark.sql("""
CREATE TABLE IF NOT EXISTS v2_ontology.graph.ontology_versions (
  domain          STRING  NOT NULL,
  version         STRING  NOT NULL,
  released_ts     TIMESTAMP,
  released_by     STRING,
  breaking_change BOOLEAN DEFAULT FALSE,
  change_summary  STRING,
  CONSTRAINT versions_pk PRIMARY KEY (domain, version)
)
USING DELTA
TBLPROPERTIES ('delta.feature.allowColumnDefaults' = 'supported')
COMMENT 'Ontology version registry per domain'
""")
print("Table created: v2_ontology.graph.ontology_versions")

# COMMAND ----------

# Create graph.change_log governance table
spark.sql("""
CREATE TABLE IF NOT EXISTS v2_ontology.graph.change_log (
  change_id         STRING    NOT NULL,
  change_ts         TIMESTAMP NOT NULL,
  changed_by        STRING,
  domain            STRING,
  object_type       STRING,
  object_id         STRING,
  change_type       STRING,
  previous_value    STRING,
  new_value         STRING,
  ticket_reference  STRING
)
USING DELTA
PARTITIONED BY (domain)
COMMENT 'Ontology change audit log'
""")
print("Table created: v2_ontology.graph.change_log")

# COMMAND ----------

# Verification: list all schemas and tables
schemas_result = spark.sql("SHOW SCHEMAS IN v2_ontology").collect()
print("\nAll schemas in v2_ontology:")
for s in schemas_result:
    schema_name = s[0]
    tables = spark.sql(f"SHOW TABLES IN v2_ontology.{schema_name}").collect()
    table_names = [t.tableName for t in tables]
    print(f"  {schema_name}: {table_names}")

print("\nTier 2 schema creation complete!")
