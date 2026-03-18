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

# MLflow tracing — import with graceful fallback
try:
    import mlflow
    mlflow.set_experiment("/ontology-platform/agent-traces")
    MLFLOW_AVAILABLE = True
    print("MLflow tracing enabled")
except Exception as e:
    MLFLOW_AVAILABLE = False
    print(f"MLflow not available: {e} — continuing without tracing")

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

# ── Domain Router ────────────────────────────────────────────────────────────
# Routes user intent to the correct domain tool set. PDF spec requirement.

DOMAIN_KEYWORDS = {
    "dietary": ["keto", "vegan", "gluten", "organic", "dairy", "paleo", "plant", "diet", "meal", "food"],
    "substitution": ["substitute", "replacement", "alternative", "out of stock", "oos", "similar"],
    "inventory": ["available", "inventory", "stock", "in stock", "on hand", "where", "store"],
    "persona": ["persona", "audience", "profile", "cohort", "customer", "household", "lifecycle"],
    "pharmacy": ["pharmacy", "prescription", "drug", "medication", "rx", "medicine"],
    "margin": ["margin", "profit", "price", "discount", "revenue", "cost"],
}

def domain_router(user_message):
    """Route user message to a primary domain. Returns list of relevant domains."""
    msg_lower = user_message.lower()
    scores = {}
    for domain, keywords in DOMAIN_KEYWORDS.items():
        scores[domain] = sum(1 for kw in keywords if kw in msg_lower)
    # Sort by score, return top 2 non-zero domains (or default to dietary)
    ranked = sorted(scores.items(), key=lambda x: x[1], reverse=True)
    active = [d for d, s in ranked if s > 0]
    return active[:2] if active else ["dietary"]

# ── Tools (PDF spec names) ────────────────────────────────────────────────────

def find_keto_products(dietary_flag="is_keto", limit=20):
    """Find products matching a dietary flag (replaces query_dietary_catalog)."""
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

# keep alias for backwards compat
query_dietary_catalog = find_keto_products

# COMMAND ----------

def get_substitutions(source_upc=None, limit=5):
    """Get substitutions ranked by composite_score (dietary_fit × margin_multiplier × similarity)."""
    if source_upc:
        result = spark.sql(f"""
            SELECT source_upc, target_upc, source_product_name, target_product_name,
                   similarity_score, composite_score, is_dietary_safe,
                   dietary_fit_score, margin_multiplier, margin_delta
            FROM v2_ontology.abstractions.margin_aware_substitution
            WHERE source_upc = '{source_upc}'
            ORDER BY composite_score DESC
            LIMIT {limit}
        """)
    else:
        result = spark.sql(f"""
            SELECT source_upc, target_upc, source_product_name, target_product_name,
                   similarity_score, composite_score, is_dietary_safe,
                   dietary_fit_score, margin_multiplier, margin_delta
            FROM v2_ontology.abstractions.margin_aware_substitution
            ORDER BY composite_score DESC
            LIMIT {limit}
        """)
    rows = result.collect()
    return json.dumps([row.asDict() for row in rows], default=str)

query_substitutions = get_substitutions  # alias

# COMMAND ----------

def check_inventory(upc=None, node_id=None, limit=10):
    """Check inventory availability for a product at a node."""
    conditions = []
    if upc:
        conditions.append(f"upc = '{upc}'")
    if node_id:
        conditions.append(f"node_id = '{node_id}'")
    where = ("WHERE " + " AND ".join(conditions)) if conditions else ""

    result = spark.sql(f"""
        SELECT upc, product_name, node_id, node_type,
               available_qty, on_hand_qty, reserved_qty,
               reorder_point, snapshot_ts
        FROM v2_ontology.abstractions.unified_inventory
        {where}
        ORDER BY available_qty DESC
        LIMIT {limit}
    """)
    rows = result.collect()
    return json.dumps([row.asDict() for row in rows], default=str)

# COMMAND ----------

def get_persona(household_id):
    """Get household persona + lifecycle stage from kpm_audiences / retail_media_audiences."""
    # Try kpm_audiences first for KetoDieter persona
    try:
        result = spark.sql(f"""
            SELECT household_id, primary_persona, lifecycle_stage,
                   keto_basket_share, keto_spend, trips_last_30d,
                   loyalty_tier, store_id_primary
            FROM v2_ontology.abstractions.kpm_audiences
            WHERE household_id = '{household_id}'
        """)
        rows = result.collect()
        if rows:
            return json.dumps([row.asDict() for row in rows], default=str)
    except Exception:
        pass

    # Fallback to retail_media_audiences
    result = spark.sql(f"""
        SELECT household_id, dietary_cohort AS primary_persona, loyalty_tier,
               age_band, has_pharmacy_rx, is_active, store_id_primary
        FROM v2_ontology.abstractions.retail_media_audiences
        WHERE household_id = '{household_id}'
    """)
    rows = result.collect()
    return json.dumps([row.asDict() for row in rows], default=str)

query_customer_context = get_persona  # alias

# COMMAND ----------

def get_kpm_audience(persona="KetoDieter", limit=50):
    """Get KPM audience segment — households matching a persona. PDF spec tool."""
    result = spark.sql(f"""
        SELECT household_id, primary_persona, lifecycle_stage,
               keto_basket_share, total_spend, trips_last_30d,
               loyalty_tier, store_id_primary
        FROM v2_ontology.abstractions.kpm_audiences
        WHERE primary_persona = '{persona}'
        ORDER BY keto_basket_share DESC
        LIMIT {limit}
    """)
    rows = result.collect()
    return json.dumps([row.asDict() for row in rows], default=str)

# COMMAND ----------

def margin_aware_sub(source_upc, min_dietary_fit=0.5, limit=5):
    """Get margin-optimised substitutions for an OOS product. PDF spec tool."""
    result = spark.sql(f"""
        SELECT source_upc, target_upc, target_product_name,
               composite_score, dietary_fit_score, margin_multiplier,
               margin_delta, similarity_score, is_dietary_safe
        FROM v2_ontology.abstractions.margin_aware_substitution
        WHERE source_upc = '{source_upc}'
          AND dietary_fit_score >= {min_dietary_fit}
        ORDER BY composite_score DESC
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

# MAGIC %md
# MAGIC ## Agent Orchestration

# COMMAND ----------

TOOLS = {
    "find_keto_products": find_keto_products,
    "get_substitutions": get_substitutions,
    "check_inventory": check_inventory,
    "get_persona": get_persona,
    "get_kpm_audience": get_kpm_audience,
    "margin_aware_sub": margin_aware_sub,
    "query_pharmacy_crosssell": query_pharmacy_crosssell,
}

def run_agent(user_message, household_id=None):
    """Agent loop with domain router, MLflow tracing, and spec-named tools."""

    run_id = str(uuid.uuid4())[:8]
    trace_data = {"run_id": run_id, "tools_called": [], "domains": []}

    # Start MLflow trace span
    mlflow_run = None
    if MLFLOW_AVAILABLE:
        try:
            mlflow_run = mlflow.start_run(run_name=f"agent-{run_id}")
            mlflow.log_param("household_id", household_id or "anonymous")
            mlflow.log_param("user_message", user_message[:200])
        except Exception:
            mlflow_run = None

    try:
        # Step 1: Domain routing — determine which tools to call
        domains = domain_router(user_message)
        trace_data["domains"] = domains
        print(f"Domain router → {domains}")

        # Step 2: Get customer persona if household provided
        customer_context = ""
        if household_id:
            customer_context = get_persona(household_id)
            trace_data["tools_called"].append("get_persona")
            print(f"Persona loaded for {household_id}")

        # Step 3: Load tools based on routing
        keto_products = ""
        if "dietary" in domains:
            keto_products = find_keto_products(dietary_flag="is_keto", limit=30)
            trace_data["tools_called"].append("find_keto_products")
            print(f"Found {len(json.loads(keto_products))} keto products")

        substitutions = ""
        if "substitution" in domains or "margin" in domains:
            substitutions = get_substitutions(limit=5)
            trace_data["tools_called"].append("get_substitutions")
            print("Substitutions (margin-aware) loaded")

        inventory_info = ""
        if "inventory" in domains:
            inventory_info = check_inventory(limit=10)
            trace_data["tools_called"].append("check_inventory")
            print("Inventory loaded")

        kpm_info = ""
        if "persona" in domains:
            kpm_info = get_kpm_audience(persona="KetoDieter", limit=20)
            trace_data["tools_called"].append("get_kpm_audience")
            print("KPM audience loaded")

        pharmacy_info = ""
        if "pharmacy" in domains and household_id:
            pharmacy_info = query_pharmacy_crosssell(household_id=household_id, limit=5)
            trace_data["tools_called"].append("query_pharmacy_crosssell")
            print("Pharmacy cross-sell loaded")

        # Step 4: Build prompt
        tool_context = f"""
CUSTOMER PERSONA (from kpm_audiences / retail_media_audiences):
{customer_context}

AVAILABLE KETO PRODUCTS (from ontology find_keto_products):
{keto_products}

SUBSTITUTION OPTIONS — MARGIN-AWARE (from margin_aware_substitution, ranked by composite_score):
{substitutions}

INVENTORY STATUS (from unified_inventory):
{inventory_info}

KPM AUDIENCE CONTEXT (from kpm_audiences):
{kpm_info}

PHARMACY CROSS-SELL (from pharmacy_grocery_crosssell):
{pharmacy_info}
"""

        full_prompt = f"""{SYSTEM_PROMPT}

{tool_context}

USER REQUEST: {user_message}

Respond with a helpful answer using the product data above. Include specific products, prices, and availability."""

        # Step 5: Call Foundation Model API
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
            trace_data["llm_status"] = "success"
        except Exception as e:
            answer = f"LLM call failed ({e}). Tool results gathered successfully - see context above."
            trace_data["llm_status"] = f"failed: {e}"

        # Log to MLflow
        if MLFLOW_AVAILABLE and mlflow_run:
            try:
                mlflow.log_param("domains", ",".join(domains))
                mlflow.log_param("tools_called", ",".join(trace_data["tools_called"]))
                mlflow.log_metric("tools_count", len(trace_data["tools_called"]))
                mlflow.log_text(answer[:2000], "agent_response.txt")
            except Exception:
                pass

        return answer

    finally:
        if MLFLOW_AVAILABLE and mlflow_run:
            try:
                mlflow.end_run()
            except Exception:
                pass

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
            "tools_used": ["find_keto_products", "get_substitutions", "check_inventory",
                           "get_persona", "get_kpm_audience", "margin_aware_sub",
                           "query_pharmacy_crosssell"],
            "domain_router": "enabled",
            "mlflow_tracing": MLFLOW_AVAILABLE
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
print(f"  Tools (PDF spec): {list(TOOLS.keys())}")
print(f"  Domain Router: enabled")
print(f"  MLflow Tracing: {MLFLOW_AVAILABLE}")
print(f"  Session ID: {session_id}")
