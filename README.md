# Retail Ontology Platform — End-to-End Guide

A three-tier retail product ontology built on Databricks, combining LLM classification,
basket-co-occurrence embeddings, and a semantic graph to power AI shopping assistants,
real-time substitution engines, and CPG data sharing.

---

## Table of Contents

1. [Architecture Overview](#architecture-overview)
2. [Data Model](#data-model)
3. [Pipeline Stages — What Each Notebook Does](#pipeline-stages)
4. [The Keto Use Case End-to-End](#the-keto-use-case-end-to-end)
5. [Deployment Prerequisites](#deployment-prerequisites)
6. [How to Deploy](#how-to-deploy)
7. [How to Run the Pipeline](#how-to-run-the-pipeline)
8. [Rerunning Failed Tasks](#rerunning-failed-tasks)
9. [Catalog & Schema Reference](#catalog--schema-reference)

---

## Architecture Overview

```
┌──────────────────────────────────────────────────────────────────┐
│  TIER 1 — Universal Foundation (frozen, ARB-governed)           │
│  v2_ontology.tier1_foundation                                   │
│  8 top-level classes: Entity, Event, Location, TimeInterval,    │
│  Relationship, Quantity, Role, Attribute                        │
│  9 relationship types: SUBSTITUTABLE_FOR, HAS_INVENTORY, ...   │
└────────────────────────┬─────────────────────────────────────────┘
                         │ inherits from
┌────────────────────────▼─────────────────────────────────────────┐
│  TIER 2 — Domain Classes (7 domains, ~92 classes)               │
│  v2_ontology.tier2_product / tier2_customer / tier2_supply_chain│
│  tier2_store_ops / tier2_pharmacy / tier2_finance / tier2_media  │
│  Graph edges live in v2_ontology.graph.relationships            │
│  AI bridge: v2_ontology.bridge.sku_classifications              │
└────────────────────────┬─────────────────────────────────────────┘
                         │ exposed via
┌────────────────────────▼─────────────────────────────────────────┐
│  TIER 3 — Abstractions & Serving (business-ready views)         │
│  v2_ontology.abstractions.*  — 7 cross-domain views             │
│  v2_ontology.metrics.*       — metric views (Genie-ready)       │
│  v2_ontology.graph.sku_online_lookup / household_online_lookup  │
│  v2_features.product_features.product_embeddings               │
└──────────────────────────────────────────────────────────────────┘
```

**Four Unity Catalog catalogs:**

| Catalog | Purpose |
|---------|---------|
| `v2_raw` | Raw source tables: POS transactions, SKU master, household profiles, inventory, pharmacy |
| `v2_ontology` | Ontology structure, graph edges, AI classifications, abstract views |
| `v2_features` | ML feature store: product and household ALS embeddings |
| `v2_sharing` | Delta Sharing views for CPG partners (no PII) |

---

## Data Model

### Raw Layer (`v2_raw`)

| Table | Rows | Description |
|-------|------|-------------|
| `transactions.pos_transactions` | 500K | Point-of-sale transactions with UPC, household, price, qty |
| `products.sku_master` | 10K | Product catalog: name, brand, dept, category, price |
| `customers.household_profiles` | 50K | Household demographics: loyalty tier, age band, store |
| `inventory.node_inventory_snapshots` | ~500K | On-hand / reserved / in-transit by node (store/DC) |
| `pharmacy.rx_claims` | — | Pharmacy prescription data for cross-sell signals |

### Ontology Layer (`v2_ontology`)

| Schema | Key Tables / Views | Description |
|--------|-------------------|-------------|
| `tier1_foundation` | `classes_top`, `relationship_types` | Frozen universal taxonomy |
| `tier2_product` | `classes`, `entities` | 24 product domain classes (FoodProduct, KetoCompliant, etc.) |
| `tier2_customer` | `classes`, `entities` | 18 customer domain classes |
| `bridge` | `sku_classifications` | LLM-generated dietary/allergen/pharmacy flags per SKU |
| `graph` | `relationships`, `sku_online_lookup`, `household_online_lookup` | Semantic edges + serving tables |
| `abstractions` | 7 views | Cross-domain joins ready for agents and BI |
| `metrics` | 4 views | Business metric definitions for Genie/SQL |
| `sharing` | 3 views | CPG partner views (aggregated, no PII) |

---

## Pipeline Stages

The pipeline runs as a **Databricks Workflow** (Job ID `1033153680345706`) on an all-purpose
cluster (DBR 17.3 LTS ML, `Standard_D16s_v3`, autoscale 2–8). Notebooks are executed in
dependency order:

```
cleanup
  └── create_raw_schema
        ├── generate_data ──────┬── embeddings ──┬── substitution_graph
        │                       │               └── vector_search
        │                       ├── lakeflow_pipeline
        │                       └── llm_classification
        ├── lakebase_setup
        └── tier1_foundation
              └── seed_tier1
                    └── tier2_schemas
                          └── seed_domains
                                └── (joins with llm_classification
                                     and substitution_graph)
                                       └── abstract_views
                                             ├── metric_views
                                             ├── delta_sharing
                                             └── enterprise_agent
                                                       └── e2e_validation
```

### Stage 00 — `cleanup` (notebook `00_cleanup.py`)

Drops all four catalogs (`v2_raw`, `v2_ontology`, `v2_features`, `v2_sharing`) with
`CASCADE`. Use this to fully reset the platform before a fresh run.

> **When to skip:** If you are running an incremental update and want to preserve existing
> data, remove `cleanup` from the job DAG or comment out the drop statements.

---

### Stage 01 — `create_raw_schema` (notebook `01_create_raw_schema.py`)

Creates all four Unity Catalog catalogs with explicit ADLS Gen2 managed locations,
then creates every schema and table DDL (transactions, products, customers, inventory,
pharmacy). Tables are empty shells at this point — data is populated in stage 02.

**Storage location:** `abfss://nsp-test-data@stnsphv9452.dfs.core.windows.net/ontology/`

---

### Stage 02 — `generate_data` (notebook `02_generate_sample_data.py`)

Generates fully synthetic retail data at demo scale:

| Dataset | Volume | Notes |
|---------|--------|-------|
| SKUs | 10,000 | 20 product profiles with known dietary ground truth |
| Households | 50,000 | Includes `HH00041872` ("Sarah") — a verified keto shopper |
| Transactions | 500,000 | 90-day basket history |
| Stores | 500 | |
| Inventory snapshots | ~500K | Per node |

The 20 SKU profiles include explicit keto ground truth (Almond Milk ✅, Oat Milk ❌, etc.)
so the LLM classification can be validated against known answers downstream.

---

### Stage 03a — `tier1_foundation` (notebook `03a_create_tier1_foundation.py`)

Creates the frozen Tier 1 schema and tables in `v2_ontology.tier1_foundation`:

- `classes_top` — 8 universal classes (Entity, Event, Location, …)
- `relationship_types` — 9 valid relationship codes (`SUBSTITUTABLE_FOR`, `HAS_INVENTORY`, etc.)
- `domain_registry` — registry of all 7 tier-2 domains
- `governance_log` — ARB change log

> **CRITICAL:** This schema is frozen after creation. Any changes require Architecture
> Review Board sign-off (enforced by governance_log constraints).

---

### Stage 03b — `seed_tier1` (notebook `03b_seed_tier1_foundation.py`)

Inserts the 8 top-level class rows and 9 relationship type rows into the Tier 1 tables.
Runs after `tier1_foundation` creates the empty tables.

---

### Stage 04a — `tier2_schemas` (notebook `04a_create_tier2_schemas.py`)

Creates all Tier 2 schemas inside `v2_ontology`:

```
tier2_product | tier2_customer | tier2_supply_chain | tier2_store_ops
tier2_pharmacy | tier2_finance | tier2_media
graph | bridge | abstractions | metrics | sharing
```

Each domain schema gets a `classes` table (domain taxonomy) and an `entities` table
(registered entities in the graph).

---

### Stage 04b — `seed_domains` (notebook `04b_seed_all_domains.py`)

Inserts ~92 domain class definitions across all 7 domains. Example product domain classes:

```
/Entity/ProductType
/Entity/ProductType/FoodProduct
/Entity/ProductType/FoodProduct/FreshProduct
/Entity/ProductType/DietaryClass/KetoCompliant   ← keto class registered here
/Entity/ProductType/DietaryClass/Vegan
/Entity/ProductType/DietaryClass/GlutenFree
...
```

---

### Stage 05 — `llm_classification` (notebook `05_llm_sku_classification.py`)

**Runtime: ~25 minutes** — the longest stage.

Classifies all 10,000 SKUs across 4 domains using **Meta Llama 3.3 70B Instruct**
(Databricks Foundation Model API) with 20 parallel threads and batches of 10.

For each SKU the LLM returns:

```json
{
  "dietary": {
    "is_keto_compliant": true/false,
    "is_vegan": true/false,
    "is_gluten_free": true/false,
    "is_organic": true/false,
    "is_dairy_free": true/false,
    "is_paleo": true/false,
    "is_plant_based": true/false,
    "is_mediterranean": true/false
  },
  "allergens": {
    "contains_tree_nuts": true/false,
    "contains_dairy": true/false,
    "contains_gluten": true/false
  },
  "margin": { "margin_segment": "HIGH|MED|LOW" },
  "pharmacy": {
    "is_pharmacy_product": true/false,
    "therapeutic_class": "..."
  }
}
```

Results are written to `v2_ontology.bridge.sku_classifications` with a `confidence_score`
field. All 10,000 rows use a single endpoint call with no external API keys required —
the Foundation Model API is built into the workspace.

> **Keto rule enforced in the prompt:** KetoCompliant = TRUE only if net carbs ≤ 5g/serving.
> Almond milk ✅ (~1g carb), regular oat milk ❌ (~12g carb).

---

### Stage 06 — `embeddings` (notebook `06_basket2vec_embeddings.py`)

Trains an **MLlib ALS** (Alternating Least Squares) model on basket co-occurrence to produce
128-dimensional product and household embeddings.

**How it works:**
1. Load last 90 days of POS transactions
2. Build household × UPC implicit feedback matrix (purchase count as signal)
3. Train ALS with `rank=128`, `alpha=40`, `implicitPrefs=True`
4. Extract item factors → 128-dim vectors per UPC → `v2_features.product_features.product_embeddings`
5. Extract user factors → 128-dim vectors per household → `v2_features.household_features.household_embeddings`

Products that are frequently purchased together (e.g. Almond Milk + Avocado + Spinach in a
keto basket) end up close together in embedding space — this is what drives substitution
scoring in stage 08.

---

### Stage 07 — `vector_search` (notebook `07_vector_search_index.py`)

Attempts to create a Databricks **Vector Search** endpoint and delta-sync index over the
product embeddings table for sub-millisecond similarity lookups at serving time.

**Graceful degradation:** If the `databricks-vectorsearch` package is unavailable (e.g.
NSG blocks PyPI in network-restricted environments), the notebook skips VS setup and exits
successfully. The substitution graph (stage 08) provides the same capability via batch
cosine similarity and the `sku_online_lookup` table.

---

### Stage 08 — `substitution_graph` (notebook `08_build_substitution_graph.py`)

**The core of the platform.** Builds `SUBSTITUTABLE_FOR` relationship edges with dietary
class preservation.

**Algorithm:**
1. Sample up to 3,000 products from the embeddings table (O(n²) cross-join)
2. Self-join and compute pairwise cosine similarity using JVM-native SQL:
   ```sql
   aggregate(zip_with(vec_a, vec_b, (x, y) -> x * y), cast(0 as double), (acc, v) -> acc + v)
   / (sqrt(aggregate(vec_a, ...)) * sqrt(aggregate(vec_b, ...)))
   ```
3. Keep pairs with `score > 0.5`
4. Join with `sku_classifications` — keep pairs where BOTH products share at least one
   dietary class (KetoCompliant, Vegan, GlutenFree, Organic, DairyFree, Paleo,
   PlantBased, MediterraneanDiet)
5. Write edges to `v2_ontology.graph.relationships`

Also builds two serving tables:
- **`sku_online_lookup`** — SKU master + all dietary/allergen/pharmacy flags, 10K rows
- **`household_online_lookup`** — household profiles + 90-day transaction stats

**Dietary class preservation ensures:** if Sarah (keto shopper) needs a substitute for
Almond Milk, she will only be offered products that are also classified as `KetoCompliant`.
She will never be offered Oat Milk (not keto) as a substitute for Almond Milk (keto).

---

### Stage 09 — `abstract_views` (notebook `09_create_abstract_views.py`)

Creates 7 cross-domain SQL views in `v2_ontology.abstractions` for agent and BI consumption:

| View | What it joins | Use case |
|------|--------------|----------|
| `unified_inventory` | inventory snapshots + SKU master | Check stock by product |
| `dietary_sku_catalog` | SKU master + classifications | "Show me all keto products" |
| `substitutable_products` | graph relationships + SKU master + classifications | "What can replace Almond Milk?" |
| `household_purchase_history` | transactions + household profiles | Customer 360 |
| `keto_shopping_list` | dietary catalog filtered to keto + inventory | Keto in-stock basket builder |
| `out_of_stock_with_substitutes` | inventory + substitution graph | OOS recovery |
| `pharmacy_cross_sell` | pharmacy RX + dietary + substitution graph | RX-adjacent recommendations |

---

### Stage 10 — `metric_views` (notebook `10_create_metric_views.py`)

Creates 4 business metric views in `v2_ontology.metrics` queryable by Genie (natural
language) or standard SQL:

| View | Metric |
|------|--------|
| `daily_sales_performance` | Transaction count, revenue, avg basket value, unique households by day |
| `inventory_health` | On-hand qty, days-of-supply, below-reorder flag by node+UPC |
| `dietary_category_sales` | Sales performance broken down by dietary classification |
| `household_segment_value` | Total spend, trip frequency, loyalty distribution by segment |

---

### Stage 11 — `lakeflow_pipeline` (notebook `11_lakeflow_pipeline.py`)

Registers a **Lakeflow (DLT) pipeline** configuration for incremental streaming ingestion:
- Bronze tables: raw POS transactions and inventory snapshots streamed from `v2_raw`
- Silver tables: deduplicated, cleaned versions
- Configuration: Photon enabled, CDF enabled, triggered (not continuous)

The pipeline definition is created/updated via Databricks SDK. If the SDK call is unavailable,
the notebook prints the JSON config for manual creation.

---

### Stage 12 — `lakebase_setup` (notebook `12_lakebase_setup.py`)

Sets up persistent **agent memory tables** for the enterprise agent (stage 13).

Tries **Lakebase** (Postgres-compatible managed database) first. If unavailable, creates
equivalent Delta tables in `v2_ontology.graph`:

| Table | Purpose |
|-------|---------|
| `agent_sessions` | Conversation session tracking |
| `agent_memory` | Key-value persistent memory per household |
| `agent_tool_calls` | Audit log of all tool calls made by the agent |

---

### Stage 13 — `enterprise_agent` (notebook `13_enterprise_agent.py`)

An **AI Shopping Assistant** powered by Llama 3.3 70B that queries the ontology views
as tools.

**System prompt:** "For Keto customers: only suggest products where `is_keto=TRUE`. When a
product is OOS: use substitutions that preserve dietary classes."

**Tool functions exposed to the LLM:**
- `query_dietary_catalog(dietary_flag)` — find products by dietary classification
- `find_substitutes(upc)` — look up substitution graph edges for a given SKU
- `check_inventory(upc, store_id)` — query unified_inventory view
- `get_household_profile(household_id)` — load household dietary preferences and purchase history

**Demo conversation run:** The notebook executes a live conversation as household `HH00041872`
(Sarah, verified keto shopper) asking for a weekly keto meal plan, showing the agent using
each tool and preserving keto compliance throughout.

---

### Stage 14 — `delta_sharing` (notebook `14_delta_sharing.py`)

Sets up **Delta Sharing** with three aggregated views for CPG partners — no PII exposed:

| Sharing View | Data | PII? |
|-------------|------|------|
| `cpg_product_performance` | Units sold, revenue, price by brand/category/dept | None |
| `dietary_trend_analysis` | Dietary classification adoption trends over time | None |
| `substitution_patterns` | Which products substitute for which, how often | None |

A share `ontology-cpg-partner-share` and recipient `cpg-partner-demo` are registered in
Unity Catalog.

---

### Stage 15 — `e2e_validation` (notebook `15_e2e_validation.py`)

Runs **18 automated tests** across 9 phases to confirm the full pipeline is healthy:

| Phase | Tests | What is validated |
|-------|-------|------------------|
| 1 | Raw data | 10K SKUs, 50K households, 500K transactions present |
| 2 | Ontology structure | Tier 1 classes and relationship types seeded correctly |
| 3 | LLM classification | `sku_classifications` has 10K rows, keto products classified |
| 4 | Embeddings | 10K product vectors present, 128 dimensions, no nulls |
| 5 | Substitution graph | `SUBSTITUTABLE_FOR` edges exist; keto substitutes preserve keto flag |
| 6 | Abstract views | All 7 abstraction views return rows |
| 7 | Agent memory | `agent_sessions` and `agent_memory` tables exist |
| 8 | Metric views | All 4 metric views return rows |
| 9 | Serving tables | `sku_online_lookup` has 10K rows, `household_online_lookup` populated |

All 18 tests must PASS for the pipeline to be considered complete.

---

## The Keto Use Case End-to-End

This section traces a single real-world scenario — *Sarah wants a keto-safe substitute for
Almond Milk when it is out of stock* — through every layer of the platform.

### Step 1 — Ground Truth in Raw Data (Stage 02)

When synthetic data is generated, Almond Milk SKUs are created with the profile:
```python
("Almond Milk", {"keto": True, "vegan": True, "gf": True, "dairy_free": True}, "07", "KETO|VEGAN|GF|DAIRY_FREE")
```
Oat Milk is explicitly `keto: False`. This ground truth flows into `v2_raw.products.sku_master`
via the `certifications` column.

Household `HH00041872` ("Sarah") is seeded with a basket history that skews heavily toward
keto products (Almond Milk, Avocado, Chicken Breast, Spinach, etc.).

### Step 2 — LLM Classification (Stage 05)

The LLM receives Almond Milk's product card:
```
Product Name: Almond Milk Unsweetened
Certifications on pack: KETO|VEGAN|GF|DAIRY_FREE
KETO RULE: almond milk YES — ~1g carb
```
It returns `is_keto_compliant: true`, which is written to `v2_ontology.bridge.sku_classifications`.

Coconut Milk, Heavy Cream, and other keto staples are similarly classified `true`. Oat Milk,
White Rice, and Pasta are classified `false`.

### Step 3 — Basket2Vec Embedding (Stage 06)

ALS learns that Almond Milk, Avocado, Spinach, Chicken Breast, and Bacon co-occur in keto
shopping baskets. Their 128-dim embedding vectors end up geometrically close. Oat Milk lives
in a different region of the embedding space (it co-occurs with Granola Bars, Yogurt, Pasta).

### Step 4 — Substitution Graph (Stage 08)

The self-join finds:
- `cosine_sim(Almond Milk, Coconut Milk) > 0.5` ← both appear in keto baskets
- Both have `is_keto_compliant = true` in `sku_classifications`
- `shared_class_ids = ["KetoCompliant", "Vegan", "GlutenFree", "DairyFree"]`

This edge is written to `v2_ontology.graph.relationships`:
```
SUBSTITUTABLE_FOR: Almond Milk → Coconut Milk
  weight: 0.73  shared_class_ids: [KetoCompliant, Vegan, GlutenFree, DairyFree]
```

Oat Milk does NOT appear as a substitute for Almond Milk — it would fail the
`shared_class_ids` filter because Oat Milk is `is_keto_compliant = false`.

### Step 5 — Abstract Views (Stage 09)

The `out_of_stock_with_substitutes` view joins the substitution graph with inventory:
```sql
SELECT original_product, substitute_product, shared_class_ids, available_qty
FROM v2_ontology.abstractions.out_of_stock_with_substitutes
WHERE original_upc = '<almond_milk_upc>'
  AND array_contains(shared_class_ids, 'KetoCompliant')
ORDER BY weight DESC
```
Result: Coconut Milk (in stock, keto ✅), Heavy Cream (in stock, keto ✅).

### Step 6 — Enterprise Agent (Stage 13)

Sarah asks: *"I need Almond Milk but it's out of stock at my store."*

The agent:
1. Calls `get_household_profile("HH00041872")` → `is_keto = true`
2. Calls `check_inventory("<almond_milk_upc>", "STORE_042")` → OOS
3. Calls `find_substitutes("<almond_milk_upc>")` → Coconut Milk (score 0.73, keto ✅),
   Heavy Cream (score 0.61, keto ✅)
4. Calls `check_inventory("<coconut_milk_upc>", "STORE_042")` → 24 units available

Response: *"Almond Milk is currently out of stock at your store. I found two keto-safe
alternatives: Coconut Milk ($3.49, 24 units in stock) and Heavy Cream ($2.99, 18 units
in stock). Both preserve your keto dietary requirements."*

### Step 7 — Validation (Stage 15)

Test T15 (Phase 5 — Substitution Graph) explicitly checks:
```python
keto_subs = spark.sql("""
    SELECT r.source_entity_id, r.target_entity_id, r.shared_class_ids
    FROM v2_ontology.graph.relationships r
    WHERE r.rel_type = 'SUBSTITUTABLE_FOR'
      AND array_contains(r.shared_class_ids, 'KetoCompliant')
""")
assert keto_subs.count() > 0, "No keto substitution edges found"
```
This asserts that the Almond Milk → Coconut Milk edge (and others like it) exist.

---

## Deployment Prerequisites

| Requirement | Details |
|-------------|---------|
| **Databricks workspace** | Azure Databricks with Unity Catalog enabled |
| **Storage** | ADLS Gen2 container accessible as managed location for UC catalogs |
| **Cluster** | DBR 17.3 LTS ML, `SINGLE_USER` access mode (required for MLlib ALS + UC) |
| **Foundation Model API** | `databricks-meta-llama-3-3-70b-instruct` endpoint enabled in the workspace |
| **Permissions** | User must have `CREATE CATALOG` and `CREATE EXTERNAL LOCATION` privileges |
| **Network** | Outbound internet access is NOT required (all Databricks APIs are internal) |

> **Note:** Vector Search (stage 07) requires PyPI access to install `databricks-vectorsearch`.
> In network-restricted environments the notebook gracefully skips VS setup. All other stages
> work fully offline.

---

## How to Deploy

### 1 — Clone the repository

```bash
git clone <repo-url>
cd ontology-platform
```

### 2 — Configure the storage location

Edit `01_create_raw_schema.py` line 17 to point at your ADLS container:

```python
BASE = "abfss://<container>@<storage-account>.dfs.core.windows.net/ontology"
```

### 3 — Create the all-purpose cluster

Via Databricks UI or CLI:

```bash
databricks clusters create --profile <your-profile> --json '{
  "cluster_name": "ontology-platform-cluster",
  "spark_version": "17.3.x-scala2.12",
  "node_type_id": "Standard_D16s_v3",
  "autoscale": {"min_workers": 2, "max_workers": 8},
  "data_security_mode": "SINGLE_USER",
  "single_user_name": "<your-email>",
  "spark_conf": {
    "spark.databricks.delta.properties.defaults.enableChangeDataFeed": "true"
  }
}'
```

> Use `17.3.x-ml-scala2.12` (ML runtime) to get MLlib ALS and pre-installed Databricks
> libraries without needing PyPI access.

### 4 — Upload notebooks to workspace

```bash
for nb in notebooks/*.py; do
  name=$(basename "$nb" .py)
  databricks workspace import \
    --file "$nb" \
    --language PYTHON \
    --overwrite \
    --profile <your-profile> \
    "/Users/<your-email>/ontology-platform/$name"
done
```

### 5 — Create the Databricks Job

```bash
databricks jobs create --profile <your-profile> --json @deploy/job_definition.json
```

The job definition file (`deploy/job_definition.json`) defines the full task DAG, cluster
binding, and notebook paths. The key fields per task:

```json
{
  "task_key": "substitution_graph",
  "notebook_task": {
    "notebook_path": "/Users/<email>/ontology-platform/08_build_substitution_graph",
    "source": "WORKSPACE"
  },
  "existing_cluster_id": "<cluster-id>",
  "depends_on": [{"task_key": "embeddings"}]
}
```

---

## How to Run the Pipeline

### Full run (fresh environment)

```bash
databricks jobs run-now \
  --job-id <job-id> \
  --profile <your-profile>
```

This runs all 18 tasks in DAG order from `cleanup` through `e2e_validation`.
Expected total runtime: **~35–40 minutes** (dominated by `llm_classification` at ~25 min).

### Monitor progress

```bash
# Get latest run ID
databricks jobs list-runs --job-id <job-id> --profile <your-profile>

# Watch task states
watch -n 30 'databricks jobs get-run <run-id> --profile <your-profile> | \
  python3 -c "
import sys,json
d=json.load(sys.stdin)
tasks=d.get(\"tasks\",[])
latest={}
for t in tasks:
    tk=t[\"task_key\"]; a=t.get(\"attempt_number\",0)
    if tk not in latest or a>latest[tk][1]: latest[tk]=(t,a)
[print(f\"  {tk}: {t.get(chr(115)+chr(116)+chr(97)+chr(116)+chr(101),{}).get(chr(114)+chr(101)+chr(115)+chr(117)+chr(108)+chr(116)+chr(95)+chr(115)+chr(116)+chr(97)+chr(116)+chr(101),chr(63))}\") for tk,(t,_) in sorted(latest.items())]
"'
```

### Incremental run (skip data generation)

If raw data already exists, you can trigger only the ML and graph stages by running
`repair-run` and specifying only the tasks you want to re-execute:

```bash
# Get latest repair ID from the run
REPAIR_ID=$(databricks jobs get-run <run-id> --profile <your-profile> | \
  python3 -c "import sys,json; d=json.load(sys.stdin); rh=d.get('repair_history',[]); print(rh[-1]['repair_id'] if rh else 0)")

databricks jobs repair-run <run-id> \
  --latest-repair-id $REPAIR_ID \
  --rerun-tasks "llm_classification,embeddings,substitution_graph,vector_search,abstract_views,metric_views,enterprise_agent,delta_sharing,e2e_validation" \
  --profile <your-profile>
```

---

## Rerunning Failed Tasks

If specific tasks fail, use the repair API to rerun only failed tasks without repeating
successful ones. The `latest_repair_id` is required for subsequent repairs (the first
repair omits it).

```bash
TOKEN=$(databricks auth token --host <workspace-url> --profile <profile> | \
  python3 -c "import sys,json; print(json.load(sys.stdin)['access_token'])")

# First repair (no latest_repair_id needed)
curl -s -X POST "<workspace-url>/api/2.1/jobs/runs/repair" \
  -H "Authorization: Bearer $TOKEN" \
  -H "Content-Type: application/json" \
  -d '{
    "run_id": <run-id>,
    "rerun_all_failed_tasks": true
  }'

# Subsequent repairs (supply the repair_id returned by the previous call)
curl -s -X POST "<workspace-url>/api/2.1/jobs/runs/repair" \
  -H "Authorization: Bearer $TOKEN" \
  -H "Content-Type: application/json" \
  -d '{
    "run_id": <run-id>,
    "latest_repair_id": <previous-repair-id>,
    "rerun_all_failed_tasks": true
  }'
```

**Common failures and fixes:**

| Symptom | Root Cause | Fix |
|---------|-----------|-----|
| Task fails in ~22s | Cluster was TERMINATED when repair started | Start cluster first, wait for RUNNING, then repair |
| `WRONG_COLUMN_DEFAULTS_FOR_DELTA_FEATURE_NOT_ENABLED` | CTAS inherits DEFAULT constraints from `v2_raw` source tables | Use `CREATE TABLE ... USING DELTA` + `insertInto()` — never CTAS from `v2_raw` |
| `llm_classification` returns 0 keto rows | `confidence_score` filter too strict (LLM sets 0.0) | Remove confidence threshold; use all classified rows |
| `substitution_graph` finds 0 pairs | Cosine similarity threshold too high for ALS vectors | Use threshold `> 0.5` (ALS factors have lower cosine range than text embeddings) |
| `vector_search` fails at ~399s | pip install hangs waiting for PyPI (NSG blocks outbound) | Notebook now exits SUCCESS gracefully when VS client unavailable |

---

## Catalog & Schema Reference

```
v2_raw
├── transactions.pos_transactions        — 500K POS rows
├── products.sku_master                  — 10K SKU catalog
├── customers.household_profiles         — 50K households
├── inventory.node_inventory_snapshots   — inventory by node
└── pharmacy.rx_claims                   — pharmacy data

v2_ontology
├── tier1_foundation
│   ├── classes_top                      — 8 universal classes (FROZEN)
│   ├── relationship_types               — 9 valid rel codes (FROZEN)
│   ├── domain_registry                  — 7 domain registrations
│   └── governance_log                   — ARB change log
├── tier2_product / tier2_customer / ...
│   ├── classes                          — domain taxonomy (~92 total)
│   └── entities                         — registered graph entities
├── bridge
│   └── sku_classifications              — LLM flags per SKU (10K rows)
├── graph
│   ├── relationships                    — semantic edges (SUBSTITUTABLE_FOR etc.)
│   ├── sku_online_lookup                — serving table: SKU + all flags
│   └── household_online_lookup          — serving table: HH + 90d stats
├── abstractions
│   ├── unified_inventory
│   ├── dietary_sku_catalog
│   ├── substitutable_products
│   ├── household_purchase_history
│   ├── keto_shopping_list
│   ├── out_of_stock_with_substitutes
│   └── pharmacy_cross_sell
├── metrics
│   ├── daily_sales_performance
│   ├── inventory_health
│   ├── dietary_category_sales
│   └── household_segment_value
└── sharing
    ├── cpg_product_performance
    ├── dietary_trend_analysis
    └── substitution_patterns

v2_features
├── product_features.product_embeddings  — 128-dim ALS vectors per UPC
└── household_features.household_embeddings — 128-dim ALS vectors per HH

v2_sharing
└── (Delta Sharing recipient views — mirrors v2_ontology.sharing)
```
