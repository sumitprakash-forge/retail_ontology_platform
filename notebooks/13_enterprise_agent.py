# Databricks notebook source
# MAGIC %md
# MAGIC # 13 - Enterprise Ontology Agent
# MAGIC A simple agent that queries v2_ontology.abstractions views using
# MAGIC Foundation Model API (databricks-meta-llama-3-3-70b-instruct) and spark.sql().

# COMMAND ----------

import json
from databricks.sdk import WorkspaceClient
from datetime import datetime
import uuid

# COMMAND ----------

# MAGIC %md
# MAGIC ## Configuration

# COMMAND ----------

LLM_ENDPOINT = "databricks-meta-llama-3-3-70b-instruct"

SYSTEM_PROMPT = """You are an AI Shopping Assistant, powered by the retail product ontology.
You have access to tools that query the ontology to find products, check inventory,
find substitutions, and understand customer dietary preferences.

KEY BEHAVIORS:
- For Keto customers: only suggest products where is_keto=TRUE
- When a product is OOS: use substitutions that preserve dietary classes
- Always check inventory before confirming availability
- For pharmacy patients: check cross-sell opportunities

When building meal plans, format as:
Day 1: Breakfast / Lunch / Dinner with specific products and prices
Total: $XX.XX | All items confirmed at [store_id]
"""

# COMMAND ----------

# MAGIC %md
# MAGIC ## Tool Functions (query v2_ontology.abstractions via spark.sql)

# COMMAND ----------

def query_dietary_catalog(dietary_flag="is_keto", limit=20):
    """Query the dietary SKU catalog for products matching a dietary flag."""
    valid_flags = ["is_keto", "is_vegan", "is_gluten_free", "is_organic", "is_dairy_free", "is_paleo", "is_plant_based"]
    if dietary_flag not in valid_flags:
        dietary_flag = "is_keto"

    result = spark.sql(f"""
        SELECT upc, product_name, brand, department_name, category_name,
               avg_retail_price, margin_segment, confidence_score
        FROM v2_ontology.abstractions.dietary_sku_catalog
        WHERE {dietary_flag} = TRUE
        ORDER BY avg_retail_price
        LIMIT {limit}
    """)
    rows = result.collect()
    return json.dumps([row.asDict() for row in rows], default=str)

# COMMAND ----------

def query_substitutions(source_upc=None, limit=5):
    """Query substitution graph for a given product UPC."""
    if source_upc:
        result = spark.sql(f"""
            SELECT source_upc, target_upc, source_product_name, target_product_name,
                   similarity_score, shared_class_ids, is_dietary_safe
            FROM v2_ontology.abstractions.substitution_graph
            WHERE source_upc = '{source_upc}'
            ORDER BY similarity_score DESC
            LIMIT {limit}
        """)
    else:
        result = spark.sql(f"""
            SELECT source_upc, target_upc, source_product_name, target_product_name,
                   similarity_score, shared_class_ids, is_dietary_safe
            FROM v2_ontology.abstractions.substitution_graph
            ORDER BY similarity_score DESC
            LIMIT {limit}
        """)
    rows = result.collect()
    return json.dumps([row.asDict() for row in rows], default=str)

# COMMAND ----------

def query_pharmacy_crosssell(household_id=None, limit=10):
    """Query pharmacy cross-sell opportunities for a household."""
    if household_id:
        where_clause = f"WHERE household_id = '{household_id}'"
    else:
        where_clause = ""

    result = spark.sql(f"""
        SELECT household_id, drug_class, drug_name,
               interaction_severity, food_interaction_detail
        FROM v2_ontology.abstractions.pharmacy_grocery_crosssell
        {where_clause}
        LIMIT {limit}
    """)
    rows = result.collect()
    return json.dumps([row.asDict() for row in rows], default=str)

# COMMAND ----------

def query_customer_context(household_id):
    """Query customer context including dietary preferences and activity."""
    result = spark.sql(f"""
        SELECT household_id, dietary_cohort, loyalty_tier, age_band,
               has_pharmacy_rx, is_active, store_id_primary
        FROM v2_ontology.abstractions.retail_media_audiences
        WHERE household_id = '{household_id}'
    """)
    rows = result.collect()
    return json.dumps([row.asDict() for row in rows], default=str)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Agent Orchestration

# COMMAND ----------

TOOLS = {
    "query_dietary_catalog": query_dietary_catalog,
    "query_substitutions": query_substitutions,
    "query_pharmacy_crosssell": query_pharmacy_crosssell,
    "query_customer_context": query_customer_context,
}

def run_agent(user_message, household_id=None):
    """Simple agent loop: gather context from tools, then generate response."""

    # Step 1: Get customer context if household provided
    customer_context = ""
    if household_id:
        customer_context = query_customer_context(household_id)
        print(f"Customer context: {customer_context}")

    # Step 2: Get keto products (default tool for meal plan requests)
    keto_products = query_dietary_catalog(dietary_flag="is_keto", limit=30)
    print(f"Found {len(json.loads(keto_products))} keto products")

    # Step 3: Check for substitutions availability
    substitutions = query_substitutions(limit=5)
    print(f"Sample substitutions loaded")

    # Step 4: Check pharmacy cross-sell if relevant
    pharmacy_info = ""
    if household_id:
        pharmacy_info = query_pharmacy_crosssell(household_id=household_id, limit=5)
        print(f"Pharmacy cross-sell info loaded")

    # Step 5: Build prompt with tool results and call LLM
    tool_context = f"""
CUSTOMER CONTEXT:
{customer_context}

AVAILABLE KETO PRODUCTS (from ontology dietary_sku_catalog):
{keto_products}

SUBSTITUTION OPTIONS (from ontology substitution_graph):
{substitutions}

PHARMACY CROSS-SELL (from ontology pharmacy_grocery_crosssell):
{pharmacy_info}
"""

    full_prompt = f"""{SYSTEM_PROMPT}

{tool_context}

USER REQUEST: {user_message}

Respond with a helpful answer using the product data above. Include specific products, prices, and availability."""

    # Call Foundation Model API
    try:
        w = WorkspaceClient()
        response = w.serving_endpoints.query(
            name=LLM_ENDPOINT,
            messages=[
                {"role": "system", "content": SYSTEM_PROMPT},
                {"role": "user", "content": full_prompt}
            ],
            max_tokens=1500,
            temperature=0.1,
        )
        answer = response.choices[0].message.content
    except Exception as e:
        answer = f"LLM call failed ({e}). Tool results gathered successfully - see context above."

    return answer

# COMMAND ----------

# MAGIC %md
# MAGIC ## Test: Keto Meal Plan for Sarah

# COMMAND ----------

test_query = "Build me a 5-day keto meal plan under $75, pickup at store STR0042"
print(f"Test query: {test_query}")
print("=" * 80)

response = run_agent(test_query, household_id="HH00041872")
print(response)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Log Agent Session

# COMMAND ----------

session_id = str(uuid.uuid4())
now = datetime.utcnow()

try:
    from pyspark.sql import Row
    session_row = Row(
        session_id=session_id,
        household_id="HH00041872",
        started_ts=now,
        ended_ts=datetime.utcnow(),
        agent_version="v1.0-fmapi",
        session_metadata=json.dumps({
            "query": test_query,
            "llm_endpoint": LLM_ENDPOINT,
            "tools_used": ["query_dietary_catalog", "query_substitutions",
                           "query_pharmacy_crosssell", "query_customer_context"]
        })
    )
    spark.createDataFrame([session_row]) \
        .write.mode("append") \
        .saveAsTable("v2_ontology.graph.agent_sessions")
    print(f"Agent session logged: {session_id}")
except Exception as e:
    print(f"Failed to log session: {e}")
    print(f"Session ID: {session_id}")

# COMMAND ----------

print("Enterprise agent test complete!")
print(f"  LLM Endpoint: {LLM_ENDPOINT}")
print(f"  Tools: {list(TOOLS.keys())}")
print(f"  Session ID: {session_id}")
