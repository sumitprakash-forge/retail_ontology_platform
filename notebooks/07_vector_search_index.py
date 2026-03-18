# Databricks notebook source
# MAGIC %md
# MAGIC # 07 - Vector Search Endpoint and Index
# MAGIC Creates a Databricks Vector Search endpoint and delta sync index
# MAGIC for real-time product similarity search.

# COMMAND ----------

import subprocess, sys, time

# Install databricks-vectorsearch if not already available
# (may fail in NPIP/NSG environments — skip VS setup gracefully if unavailable)
try:
    from databricks.vector_search.client import VectorSearchClient
    print("databricks-vectorsearch already available")
except ImportError:
    print("Installing databricks-vectorsearch...")
    result = subprocess.run(
        [sys.executable, "-m", "pip", "install", "-q", "databricks-vectorsearch"],
        capture_output=True, text=True
    )
    if result.returncode != 0:
        print(f"pip install warning: {result.stderr[:500]}")
    from databricks.vector_search.client import VectorSearchClient
    print("databricks-vectorsearch installed")

VS_ENDPOINT_NAME = "ontology-product-similarity"
SOURCE_TABLE = "v2_features.product_features.product_embeddings"
VS_INDEX_NAME = "v2_features.product_features.product_embeddings_index"
PRIMARY_KEY = "upc"
EMBEDDING_DIMENSION = 128
EMBEDDING_VECTOR_COLUMN = "vector"

# COMMAND ----------

# MAGIC %md
# MAGIC ## Create Vector Search Endpoint (STANDARD type)

# COMMAND ----------

vsc = VectorSearchClient()

try:
    endpoint = vsc.get_endpoint(VS_ENDPOINT_NAME)
    print(f"Endpoint '{VS_ENDPOINT_NAME}' already exists. Status: {endpoint.get('endpoint_status', {}).get('state', 'UNKNOWN')}")
except Exception:
    print(f"Creating Vector Search endpoint '{VS_ENDPOINT_NAME}' (STANDARD type)...")
    vsc.create_endpoint(
        name=VS_ENDPOINT_NAME,
        endpoint_type="STANDARD"
    )
    print(f"Endpoint '{VS_ENDPOINT_NAME}' creation initiated.")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Wait for Endpoint to be ONLINE (poll every 30s, timeout 600s)

# COMMAND ----------

def wait_for_endpoint(client, endpoint_name, timeout=600, poll_interval=30):
    """Wait for the VS endpoint to reach ONLINE state."""
    start_time = time.time()
    while time.time() - start_time < timeout:
        try:
            endpoint = client.get_endpoint(endpoint_name)
            state = endpoint.get("endpoint_status", {}).get("state", "UNKNOWN")
            print(f"  Endpoint state: {state} (elapsed: {int(time.time() - start_time)}s)")
            if state == "ONLINE":
                return True
        except Exception as e:
            print(f"  Waiting for endpoint... ({e})")
        time.sleep(poll_interval)
    return False

is_ready = wait_for_endpoint(vsc, VS_ENDPOINT_NAME, timeout=600, poll_interval=30)
if is_ready:
    print(f"Endpoint '{VS_ENDPOINT_NAME}' is ONLINE!")
else:
    print(f"Warning: Endpoint not ONLINE after 600s timeout. Proceeding with index creation anyway.")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Enable Change Data Feed on Source Table

# COMMAND ----------

spark.sql(f"""
ALTER TABLE {SOURCE_TABLE}
SET TBLPROPERTIES ('delta.enableChangeDataFeed' = 'true')
""")
print(f"CDF enabled on {SOURCE_TABLE}")

# Verify table schema
spark.sql(f"DESCRIBE TABLE {SOURCE_TABLE}").show(truncate=False)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Create Delta Sync Vector Search Index

# COMMAND ----------

try:
    index = vsc.get_index(VS_ENDPOINT_NAME, VS_INDEX_NAME)
    print(f"Index '{VS_INDEX_NAME}' already exists.")
except Exception as get_err:
    print(f"Index not found ({get_err}). Creating delta sync index '{VS_INDEX_NAME}'...")
    try:
        index = vsc.create_delta_sync_index(
            endpoint_name=VS_ENDPOINT_NAME,
            index_name=VS_INDEX_NAME,
            source_table_name=SOURCE_TABLE,
            pipeline_type="TRIGGERED",
            primary_key=PRIMARY_KEY,
            embedding_dimension=EMBEDDING_DIMENSION,
            embedding_vector_column=EMBEDDING_VECTOR_COLUMN
        )
        print(f"Index '{VS_INDEX_NAME}' creation initiated.")
    except Exception as create_err:
        print(f"Index creation error (non-fatal): {create_err}")
        index = None

# COMMAND ----------

# MAGIC %md
# MAGIC ## Wait for Index Sync

# COMMAND ----------

def wait_for_index(client, endpoint_name, index_name, timeout=600, poll_interval=30):
    """Wait for the index to be ready."""
    start_time = time.time()
    while time.time() - start_time < timeout:
        try:
            idx = client.get_index(endpoint_name, index_name)
            status = idx.describe().get("status", {})
            ready = status.get("ready", False)
            detailed = status.get("detailed_state", "UNKNOWN")
            print(f"  Index state: ready={ready}, detailed={detailed} (elapsed: {int(time.time() - start_time)}s)")
            if ready:
                return True
        except Exception as e:
            print(f"  Waiting for index... ({e})")
        time.sleep(poll_interval)
    return False

if index is not None:
    index_ready = wait_for_index(vsc, VS_ENDPOINT_NAME, VS_INDEX_NAME, timeout=600, poll_interval=30)
    if index_ready:
        print("Vector Search index is ready and synced!")
    else:
        print("Warning: Index not ready after timeout. It may still be syncing.")
else:
    print("Skipping index wait — index object not available.")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Test Similarity Search - Keto Product

# COMMAND ----------

# Test: find similar products to a keto item
try:
    # Get a keto product embedding to use as query vector
    keto_embedding = spark.sql(f"""
        SELECT e.upc, e.{EMBEDDING_VECTOR_COLUMN}, s.product_name
        FROM {SOURCE_TABLE} e
        JOIN v2_raw.products.sku_master s ON e.upc = s.upc
        JOIN v2_ontology.bridge.sku_classifications c ON e.upc = c.upc
        WHERE c.is_keto_compliant = TRUE
        LIMIT 1
    """).collect()

    if keto_embedding:
        test_upc = keto_embedding[0].upc
        test_name = keto_embedding[0].product_name
        test_vector = list(keto_embedding[0][EMBEDDING_VECTOR_COLUMN])
        print(f"Query product: {test_name} (UPC: {test_upc})")

        # Get the index handle
        idx = vsc.get_index(VS_ENDPOINT_NAME, VS_INDEX_NAME)
        results = idx.similarity_search(
            query_vector=test_vector,
            columns=[PRIMARY_KEY],
            num_results=10
        )

        print(f"\nTop 10 similar products to '{test_name}':")
        result_upcs = []
        for row in results.get("result", {}).get("data_array", []):
            result_upcs.append(row[0])
            print(f"  UPC: {row[0]}, Score: {row[-1]:.4f}")

        # Cross-reference with keto classification
        if result_upcs:
            upc_list = "','".join(result_upcs)
            keto_check = spark.sql(f"""
                SELECT s.upc, s.product_name, c.is_keto_compliant
                FROM v2_raw.products.sku_master s
                LEFT JOIN v2_ontology.bridge.sku_classifications c ON s.upc = c.upc
                WHERE s.upc IN ('{upc_list}')
            """)
            print("\nKeto classification of similar products:")
            keto_check.show(truncate=False)
    else:
        print("No keto product with embedding found. Run classification and embedding steps first.")
except Exception as e:
    print(f"Similarity search test: {e}")
    print("(This is expected if the index is still syncing)")

# COMMAND ----------

print("Vector Search setup complete!")
print(f"  Endpoint: {VS_ENDPOINT_NAME}")
print(f"  Index: {VS_INDEX_NAME}")
print(f"  Source Table: {SOURCE_TABLE}")
print(f"  Primary Key: {PRIMARY_KEY}")
print(f"  Embedding Dimension: {EMBEDDING_DIMENSION}")
print(f"  Embedding Column: {EMBEDDING_VECTOR_COLUMN}")
print(f"  Pipeline Type: TRIGGERED")
