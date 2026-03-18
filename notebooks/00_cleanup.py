# Databricks notebook source
# TITLE: 00_cleanup.py
# Drop all tables and schemas for v2_raw, v2_ontology, v2_features, and v2_sharing catalogs

# COMMAND ----------

# Drop all 4 catalogs with CASCADE (also drops all schemas and tables inside)
# Catalogs are recreated with proper MANAGED LOCATION in 01_create_raw_schema

for catalog in ["v2_raw", "v2_ontology", "v2_features", "v2_sharing"]:
    try:
        spark.sql(f"DROP CATALOG IF EXISTS {catalog} CASCADE")
        print(f"  Dropped catalog {catalog}")
    except Exception as e:
        print(f"  {catalog}: {e}")

# COMMAND ----------

print("\nCleanup complete. All v2 catalogs dropped.")
