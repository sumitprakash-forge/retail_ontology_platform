# Databricks notebook source
# MAGIC %md
# MAGIC # 13 - Enterprise Ontology Agent (LangGraph)
# MAGIC LangGraph state machine with domain router, MLflow tracing, and
# MAGIC Foundation Model API (databricks-meta-llama-3-3-70b-instruct).
# MAGIC PDF spec: Layer 4 — Agent & Reasoning.

# COMMAND ----------

import json, operator, uuid
from datetime import datetime
from typing import TypedDict, Annotated
from databricks.sdk import WorkspaceClient

# ── MLflow tracing ─────────────────────────────────────────────────────────
try:
    import mlflow
    mlflow.set_experiment("/ontology-platform/agent-traces")
    MLFLOW_AVAILABLE = True
    print("MLflow tracing enabled")
except Exception as e:
    MLFLOW_AVAILABLE = False
    print(f"MLflow not available: {e} — continuing without tracing")

# ── LangGraph install (graceful fallback) ───────────────────────────────────
import subprocess, sys
LANGGRAPH_AVAILABLE = False
try:
    from langgraph.graph import StateGraph, END, START
    LANGGRAPH_AVAILABLE = True
    print("LangGraph already available")
except ImportError:
    print("Installing langgraph...")
    try:
        r = subprocess.run(
            [sys.executable, "-m", "pip", "install", "-q",
             "--timeout=60", "langgraph>=0.2"],
            capture_output=True, text=True, timeout=180
        )
        if r.returncode == 0:
            from langgraph.graph import StateGraph, END, START
            LANGGRAPH_AVAILABLE = True
            print("LangGraph installed successfully")
        else:
            print(f"LangGraph install failed: {r.stderr[:300]}")
    except Exception as install_err:
        print(f"LangGraph install error: {install_err}")

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
# MAGIC ## Domain Router

# COMMAND ----------

DOMAIN_KEYWORDS = {
    "dietary":      ["keto", "vegan", "gluten", "organic", "dairy", "paleo", "plant", "diet", "meal", "food"],
    "substitution": ["substitute", "replacement", "alternative", "out of stock", "oos", "similar"],
    "inventory":    ["available", "inventory", "stock", "in stock", "on hand", "where", "store"],
    "persona":      ["persona", "audience", "profile", "cohort", "customer", "household", "lifecycle"],
    "pharmacy":     ["pharmacy", "prescription", "drug", "medication", "rx", "medicine"],
    "margin":       ["margin", "profit", "price", "discount", "revenue", "cost"],
}

def domain_router(user_message):
    """Score message against domain keywords. Returns top-2 active domains."""
    msg_lower = user_message.lower()
    scores = {d: sum(1 for kw in kws if kw in msg_lower) for d, kws in DOMAIN_KEYWORDS.items()}
    ranked = sorted(scores.items(), key=lambda x: x[1], reverse=True)
    active = [d for d, s in ranked if s > 0]
    return active[:2] if active else ["dietary"]

# COMMAND ----------

# MAGIC %md
# MAGIC ## Ontology Tool Functions

# COMMAND ----------

def find_keto_products(dietary_flag="is_keto", limit=20):
    """Find products matching a dietary flag."""
    valid = ["is_keto","is_vegan","is_gluten_free","is_organic","is_dairy_free","is_paleo","is_plant_based"]
    if dietary_flag not in valid:
        dietary_flag = "is_keto"
    rows = spark.sql(f"""
        SELECT upc, product_name, brand, department_name, category_name,
               avg_retail_price, margin_segment, confidence_score
        FROM v2_ontology.abstractions.dietary_sku_catalog
        WHERE {dietary_flag} = TRUE
        ORDER BY avg_retail_price LIMIT {limit}
    """).collect()
    return json.dumps([r.asDict() for r in rows], default=str)

def get_substitutions(source_upc=None, limit=5):
    """Get margin-aware substitutions ranked by composite_score."""
    where = f"WHERE source_upc = '{source_upc}'" if source_upc else ""
    rows = spark.sql(f"""
        SELECT source_upc, target_upc, source_product_name, target_product_name,
               similarity_score, composite_score, is_dietary_safe,
               dietary_fit_score, margin_multiplier, margin_delta
        FROM v2_ontology.abstractions.margin_aware_substitution
        {where}
        ORDER BY composite_score DESC LIMIT {limit}
    """).collect()
    return json.dumps([r.asDict() for r in rows], default=str)

def check_inventory(upc=None, node_id=None, limit=10):
    """Check inventory availability."""
    conds = []
    if upc:     conds.append(f"upc = '{upc}'")
    if node_id: conds.append(f"node_id = '{node_id}'")
    where = ("WHERE " + " AND ".join(conds)) if conds else ""
    rows = spark.sql(f"""
        SELECT upc, product_name, node_id, node_type,
               available_qty, on_hand_qty, reserved_qty, reorder_point, snapshot_ts
        FROM v2_ontology.abstractions.unified_inventory
        {where}
        ORDER BY available_qty DESC LIMIT {limit}
    """).collect()
    return json.dumps([r.asDict() for r in rows], default=str)

def get_persona(household_id):
    """Get household persona from kpm_audiences (falls back to retail_media_audiences)."""
    try:
        rows = spark.sql(f"""
            SELECT household_id, primary_persona, lifecycle_stage,
                   keto_basket_share, keto_spend, trips_last_30d,
                   loyalty_tier, store_id_primary
            FROM v2_ontology.abstractions.kpm_audiences
            WHERE household_id = '{household_id}'
        """).collect()
        if rows:
            return json.dumps([r.asDict() for r in rows], default=str)
    except Exception:
        pass
    rows = spark.sql(f"""
        SELECT household_id, dietary_cohort AS primary_persona, loyalty_tier,
               age_band, has_pharmacy_rx, is_active, store_id_primary
        FROM v2_ontology.abstractions.retail_media_audiences
        WHERE household_id = '{household_id}'
    """).collect()
    return json.dumps([r.asDict() for r in rows], default=str)

def get_kpm_audience(persona="KetoDieter", limit=50):
    """Get KPM audience segment for a persona."""
    rows = spark.sql(f"""
        SELECT household_id, primary_persona, lifecycle_stage,
               keto_basket_share, total_spend, trips_last_30d,
               loyalty_tier, store_id_primary
        FROM v2_ontology.abstractions.kpm_audiences
        WHERE primary_persona = '{persona}'
        ORDER BY keto_basket_share DESC LIMIT {limit}
    """).collect()
    return json.dumps([r.asDict() for r in rows], default=str)

def margin_aware_sub(source_upc, min_dietary_fit=0.5, limit=5):
    """Get margin-optimised substitutions for an OOS product."""
    rows = spark.sql(f"""
        SELECT source_upc, target_upc, target_product_name,
               composite_score, dietary_fit_score, margin_multiplier,
               margin_delta, similarity_score, is_dietary_safe
        FROM v2_ontology.abstractions.margin_aware_substitution
        WHERE source_upc = '{source_upc}'
          AND dietary_fit_score >= {min_dietary_fit}
        ORDER BY composite_score DESC LIMIT {limit}
    """).collect()
    return json.dumps([r.asDict() for r in rows], default=str)

def query_pharmacy_crosssell(household_id=None, limit=10):
    """Query pharmacy cross-sell opportunities."""
    where = f"WHERE household_id = '{household_id}'" if household_id else ""
    rows = spark.sql(f"""
        SELECT household_id, drug_class, drug_name,
               interaction_severity, food_interaction_detail
        FROM v2_ontology.abstractions.pharmacy_grocery_crosssell
        {where} LIMIT {limit}
    """).collect()
    return json.dumps([r.asDict() for r in rows], default=str)

# keep aliases
query_dietary_catalog = find_keto_products
query_substitutions   = get_substitutions
query_customer_context = get_persona

TOOLS = {
    "find_keto_products":      find_keto_products,
    "get_substitutions":       get_substitutions,
    "check_inventory":         check_inventory,
    "get_persona":             get_persona,
    "get_kpm_audience":        get_kpm_audience,
    "margin_aware_sub":        margin_aware_sub,
    "query_pharmacy_crosssell": query_pharmacy_crosssell,
}

# COMMAND ----------

# MAGIC %md
# MAGIC ## LangGraph State Machine
# MAGIC
# MAGIC Graph topology (PDF spec — Layer 4):
# MAGIC ```
# MAGIC START → route_node → persona_node → dietary_node → substitution_node
# MAGIC                                                          ↓
# MAGIC                                   pharmacy_node ← kpm_node ← inventory_node
# MAGIC                                          ↓
# MAGIC                                       llm_node → END
# MAGIC ```
# MAGIC Each node checks state.domains before executing (conditional skip).

# COMMAND ----------

if LANGGRAPH_AVAILABLE:

    class AgentState(TypedDict):
        """Full state threaded through the LangGraph state machine."""
        user_message:     str
        household_id:     str
        domains:          list
        customer_context: str
        keto_products:    str
        substitutions:    str
        inventory_info:   str
        kpm_info:         str
        pharmacy_info:    str
        tools_called:     Annotated[list, operator.add]
        response:         str

    # ── Node functions ──────────────────────────────────────────────────────

    def route_node(state: AgentState) -> dict:
        """Domain router — scores intent and sets active domains."""
        domains = domain_router(state["user_message"])
        print(f"[route_node] domains → {domains}")
        return {"domains": domains}

    def persona_node(state: AgentState) -> dict:
        """Retrieve household persona if household_id provided."""
        hid = state.get("household_id", "")
        if not hid:
            return {"customer_context": ""}
        ctx = get_persona(hid)
        print(f"[persona_node] loaded persona for {hid}")
        return {"customer_context": ctx, "tools_called": ["get_persona"]}

    def dietary_node(state: AgentState) -> dict:
        """Find keto/dietary products if dietary domain active."""
        if "dietary" not in state.get("domains", []):
            return {"keto_products": ""}
        products = find_keto_products(dietary_flag="is_keto", limit=30)
        n = len(json.loads(products))
        print(f"[dietary_node] found {n} keto products")
        return {"keto_products": products, "tools_called": ["find_keto_products"]}

    def substitution_node(state: AgentState) -> dict:
        """Load margin-aware substitutions if substitution/margin domain active."""
        if not any(d in state.get("domains", []) for d in ["substitution", "margin"]):
            return {"substitutions": ""}
        subs = get_substitutions(limit=5)
        print(f"[substitution_node] loaded margin-aware substitutions")
        return {"substitutions": subs, "tools_called": ["get_substitutions"]}

    def inventory_node(state: AgentState) -> dict:
        """Check inventory if inventory domain active."""
        if "inventory" not in state.get("domains", []):
            return {"inventory_info": ""}
        inv = check_inventory(limit=10)
        print(f"[inventory_node] inventory loaded")
        return {"inventory_info": inv, "tools_called": ["check_inventory"]}

    def kpm_node(state: AgentState) -> dict:
        """Load KPM audience if persona domain active."""
        if "persona" not in state.get("domains", []):
            return {"kpm_info": ""}
        kpm = get_kpm_audience(persona="KetoDieter", limit=20)
        print(f"[kpm_node] KPM audience loaded")
        return {"kpm_info": kpm, "tools_called": ["get_kpm_audience"]}

    def pharmacy_node(state: AgentState) -> dict:
        """Load pharmacy cross-sell if pharmacy domain active and household known."""
        hid = state.get("household_id", "")
        if "pharmacy" not in state.get("domains", []) or not hid:
            return {"pharmacy_info": ""}
        pharm = query_pharmacy_crosssell(household_id=hid, limit=5)
        print(f"[pharmacy_node] pharmacy cross-sell loaded")
        return {"pharmacy_info": pharm, "tools_called": ["query_pharmacy_crosssell"]}

    def llm_node(state: AgentState) -> dict:
        """Gather all tool outputs and call Foundation Model API."""
        tool_context = f"""
CUSTOMER PERSONA (from kpm_audiences / retail_media_audiences):
{state.get('customer_context', '')}

AVAILABLE KETO PRODUCTS (find_keto_products → dietary_sku_catalog):
{state.get('keto_products', '')}

SUBSTITUTION OPTIONS — MARGIN-AWARE (composite_score = dietary_fit × margin × similarity):
{state.get('substitutions', '')}

INVENTORY STATUS (unified_inventory):
{state.get('inventory_info', '')}

KPM AUDIENCE (kpm_audiences — KetoDieter households):
{state.get('kpm_info', '')}

PHARMACY CROSS-SELL (pharmacy_grocery_crosssell):
{state.get('pharmacy_info', '')}
"""
        full_prompt = (
            f"{SYSTEM_PROMPT}\n\n{tool_context}\n\n"
            f"USER REQUEST: {state['user_message']}\n\n"
            "Respond with a helpful answer using the product data above. "
            "Include specific products, prices, and availability."
        )
        try:
            w = WorkspaceClient()
            resp = w.serving_endpoints.query(
                name=LLM_ENDPOINT,
                messages=[
                    {"role": "system", "content": SYSTEM_PROMPT},
                    {"role": "user",   "content": full_prompt},
                ],
                max_tokens=1500,
                temperature=0.1,
            )
            answer = resp.choices[0].message.content
            print(f"[llm_node] LLM response generated ({len(answer)} chars)")
        except Exception as e:
            answer = f"LLM call failed ({e}). Tool results gathered — see context above."
            print(f"[llm_node] LLM error: {e}")
        return {"response": answer, "tools_called": ["llm"]}

    # ── Build StateGraph ────────────────────────────────────────────────────

    builder = StateGraph(AgentState)

    # Add nodes
    builder.add_node("route_node",       route_node)
    builder.add_node("persona_node",     persona_node)
    builder.add_node("dietary_node",     dietary_node)
    builder.add_node("substitution_node", substitution_node)
    builder.add_node("inventory_node",   inventory_node)
    builder.add_node("kpm_node",         kpm_node)
    builder.add_node("pharmacy_node",    pharmacy_node)
    builder.add_node("llm_node",         llm_node)

    # Edges: START → route → sequential tool chain → llm → END
    builder.add_edge(START,               "route_node")
    builder.add_edge("route_node",        "persona_node")
    builder.add_edge("persona_node",      "dietary_node")
    builder.add_edge("dietary_node",      "substitution_node")
    builder.add_edge("substitution_node", "inventory_node")
    builder.add_edge("inventory_node",    "kpm_node")
    builder.add_edge("kpm_node",          "pharmacy_node")
    builder.add_edge("pharmacy_node",     "llm_node")
    builder.add_edge("llm_node",          END)

    # Compile
    agent_graph = builder.compile()
    print("LangGraph state machine compiled successfully")
    print(f"  Nodes: {list(builder.nodes.keys())}")

else:
    agent_graph = None
    print("LangGraph not available — using fallback run_agent()")

# COMMAND ----------

# MAGIC %md
# MAGIC ## run_agent — LangGraph execution with MLflow tracing

# COMMAND ----------

def run_agent(user_message, household_id=None):
    """Run agent via LangGraph state machine. Falls back to direct loop if unavailable."""

    run_id = str(uuid.uuid4())[:8]
    mlflow_run = None

    if MLFLOW_AVAILABLE:
        try:
            mlflow_run = mlflow.start_run(run_name=f"agent-langgraph-{run_id}")
            mlflow.log_param("household_id", household_id or "anonymous")
            mlflow.log_param("user_message",  user_message[:200])
            mlflow.log_param("langgraph",     LANGGRAPH_AVAILABLE)
        except Exception:
            mlflow_run = None

    try:
        if LANGGRAPH_AVAILABLE and agent_graph is not None:
            # ── LangGraph execution path ──────────────────────────────────
            initial_state = {
                "user_message":     user_message,
                "household_id":     household_id or "",
                "domains":          [],
                "customer_context": "",
                "keto_products":    "",
                "substitutions":    "",
                "inventory_info":   "",
                "kpm_info":         "",
                "pharmacy_info":    "",
                "tools_called":     [],
                "response":         "",
            }
            final_state = agent_graph.invoke(initial_state)
            answer       = final_state["response"]
            tools_called = final_state.get("tools_called", [])
            domains      = final_state.get("domains", [])

        else:
            # ── Fallback: direct tool dispatch ────────────────────────────
            domains = domain_router(user_message)
            tools_called = []
            print(f"[fallback] domains → {domains}")

            customer_context = ""
            if household_id:
                customer_context = get_persona(household_id)
                tools_called.append("get_persona")

            keto_products = find_keto_products(limit=30) if "dietary" in domains else ""
            if keto_products: tools_called.append("find_keto_products")

            substitutions = get_substitutions(limit=5) if any(
                d in domains for d in ["substitution","margin"]) else ""
            if substitutions: tools_called.append("get_substitutions")

            inventory_info = check_inventory(limit=10) if "inventory" in domains else ""
            if inventory_info: tools_called.append("check_inventory")

            kpm_info = get_kpm_audience(limit=20) if "persona" in domains else ""
            if kpm_info: tools_called.append("get_kpm_audience")

            pharmacy_info = ""
            if "pharmacy" in domains and household_id:
                pharmacy_info = query_pharmacy_crosssell(household_id=household_id, limit=5)
                tools_called.append("query_pharmacy_crosssell")

            tool_context = (
                f"CUSTOMER PERSONA:\n{customer_context}\n\n"
                f"KETO PRODUCTS:\n{keto_products}\n\n"
                f"SUBSTITUTIONS:\n{substitutions}\n\n"
                f"INVENTORY:\n{inventory_info}\n\n"
                f"KPM AUDIENCE:\n{kpm_info}\n\n"
                f"PHARMACY:\n{pharmacy_info}"
            )
            try:
                w = WorkspaceClient()
                resp = w.serving_endpoints.query(
                    name=LLM_ENDPOINT,
                    messages=[
                        {"role": "system", "content": SYSTEM_PROMPT},
                        {"role": "user",   "content": f"{SYSTEM_PROMPT}\n\n{tool_context}\n\nUSER REQUEST: {user_message}"},
                    ],
                    max_tokens=1500, temperature=0.1,
                )
                answer = resp.choices[0].message.content
            except Exception as e:
                answer = f"LLM call failed ({e}). Tools: {tools_called}"

        # ── MLflow logging ──────────────────────────────────────────────
        if MLFLOW_AVAILABLE and mlflow_run:
            try:
                mlflow.log_param("domains",      ",".join(domains))
                mlflow.log_param("tools_called", ",".join(tools_called))
                mlflow.log_metric("tools_count",  len(tools_called))
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
# MAGIC ## Test: Keto Meal Plan for Sarah (HH00041872)

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
        agent_version="v2.0-langgraph",
        session_metadata=json.dumps({
            "query":            test_query,
            "llm_endpoint":     LLM_ENDPOINT,
            "langgraph":        LANGGRAPH_AVAILABLE,
            "mlflow_tracing":   MLFLOW_AVAILABLE,
            "tools":            list(TOOLS.keys()),
            "orchestration":    "LangGraph StateGraph" if LANGGRAPH_AVAILABLE else "fallback",
        })
    )
    spark.createDataFrame([session_row]) \
        .write.mode("append") \
        .saveAsTable("v2_ontology.graph.agent_sessions")
    print(f"Agent session logged: {session_id}")
except Exception as e:
    print(f"Failed to log session: {e}")

# COMMAND ----------

print("\nEnterprise agent complete!")
print(f"  Orchestration: {'LangGraph StateGraph' if LANGGRAPH_AVAILABLE else 'Fallback loop'}")
print(f"  LLM Endpoint:  {LLM_ENDPOINT}")
print(f"  Tools:         {list(TOOLS.keys())}")
print(f"  Domain Router: enabled")
print(f"  MLflow Tracing:{MLFLOW_AVAILABLE}")
print(f"  Session ID:    {session_id}")
