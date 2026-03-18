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
# MAGIC ## Verify Tables

# COMMAND ----------

print("\n=== Agent Memory Tables Verification ===")
for table_name in ["agent_sessions", "agent_memory"]:
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
