# Databricks notebook source
# 05 - LLM SKU Classification (10K SKUs)
# Uses Foundation Model API (Meta Llama 3.3 70B Instruct) to classify SKUs
# across dietary, allergen, margin, and pharmacy domains per spec Section 7.
# Writes to v2_ontology.bridge.sku_classifications.

# COMMAND ----------

import json
import re
import time
import threading
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime

from databricks.sdk import WorkspaceClient
from pyspark.sql import functions as F
from pyspark.sql.types import *

ENDPOINT = "databricks-meta-llama-3-3-70b-instruct"
BATCH_SIZE = 10
MAX_RETRIES = 3
NUM_THREADS = 20

# COMMAND ----------

# Multi-domain classification prompt per spec Section 7
CLASSIFICATION_PROMPT = """You are a retail product ontologist for a major supermarket chain.
You work with a three-tier ontology. Classify this SKU across ALL relevant domains.
Be strict: only mark TRUE if highly confident.

Product Name: {product_name}
Brand: {brand}
Department: {department_name} (code: {department_code})
Ingredients/Description: {ingredients_text}
Certifications on pack: {certifications}
Avg Retail Price: ${avg_retail_price}
Is Private Label: {is_private_label}

KETO RULE: KetoCompliant = TRUE only if net carbs <= 5g per serving OR
  product is pure protein/fat with no starchy carbs
  (chicken, beef, fish, most cheeses, leafy vegetables, avocado, nuts = YES)
  (bread, pasta, rice, potatoes, milk, oat milk, fruit juice = NO)
  (almond milk YES — ~1g carb; regular milk NO — ~12g carb)

Return ONLY valid JSON, no markdown:
{{
  "dietary": {{
    "is_keto_compliant": false,
    "is_vegan": false,
    "is_gluten_free": false,
    "is_organic": false,
    "is_dairy_free": false,
    "is_paleo": false,
    "is_plant_based": false,
    "is_mediterranean": false
  }},
  "allergens": {{
    "contains_tree_nuts": false,
    "contains_dairy": false,
    "contains_gluten": false,
    "contains_shellfish": false,
    "contains_soy": false,
    "contains_eggs": false
  }},
  "margin_segment": "HighMarginCategory",
  "is_pharmacy_product": false,
  "therapeutic_class": null,
  "confidence_score": 0.92,
  "classification_notes": "brief reasoning"
}}"""

# COMMAND ----------

# Result schema matching bridge.sku_classifications
result_schema = StructType([
    StructField("upc", StringType()),
    StructField("is_keto_compliant", BooleanType()),
    StructField("is_vegan", BooleanType()),
    StructField("is_gluten_free", BooleanType()),
    StructField("is_organic", BooleanType()),
    StructField("is_dairy_free", BooleanType()),
    StructField("is_paleo", BooleanType()),
    StructField("is_plant_based", BooleanType()),
    StructField("is_mediterranean", BooleanType()),
    StructField("contains_tree_nuts", BooleanType()),
    StructField("contains_dairy", BooleanType()),
    StructField("contains_gluten", BooleanType()),
    StructField("contains_shellfish", BooleanType()),
    StructField("contains_soy", BooleanType()),
    StructField("contains_eggs", BooleanType()),
    StructField("margin_segment", StringType()),
    StructField("is_pharmacy_product", BooleanType()),
    StructField("therapeutic_class", StringType()),
    StructField("confidence_score", DoubleType()),
    StructField("classification_notes", StringType()),
    StructField("classification_ts", TimestampType()),
    StructField("model_used", StringType()),
])

# COMMAND ----------

# Load 10K SKUs from v2_raw
skus_df = spark.sql("""
    SELECT upc, product_name, brand, department_name, department_code,
           ingredients_text, certifications, avg_retail_price, is_private_label
    FROM v2_raw.products.sku_master
    LIMIT 10000
""")

total_skus = skus_df.count()
print(f"Total SKUs to classify: {total_skus}")

skus_list = skus_df.collect()

# COMMAND ----------

# Classification function using mlflow.deployments client
def classify_batch(batch_rows):
    """Classify a batch of SKU rows via Foundation Model API."""
    w = WorkspaceClient()
    results = []

    for row in batch_rows:
        prompt = CLASSIFICATION_PROMPT.format(
            product_name=row["product_name"] or "Unknown",
            brand=row["brand"] or "Unknown",
            department_name=row["department_name"] or "Unknown",
            department_code=row["department_code"] or "00",
            ingredients_text=(row["ingredients_text"] or "Not provided")[:500],
            certifications=row["certifications"] or "None",
            avg_retail_price=row["avg_retail_price"] or 0,
            is_private_label=row["is_private_label"] or False,
        )

        classified = False
        for attempt in range(MAX_RETRIES):
            try:
                response = w.serving_endpoints.query(
                    name=ENDPOINT,
                    messages=[{"role": "user", "content": prompt}],
                    max_tokens=300,
                    temperature=0.0,
                )

                # Extract content from response
                try:
                    raw = response.choices[0].message.content
                except (AttributeError, TypeError):
                    try:
                        raw = response["choices"][0]["message"]["content"]
                    except (KeyError, TypeError):
                        rd = response if isinstance(response, dict) else response.__dict__
                        raw = rd["choices"][0]["message"]["content"]

                # Strip markdown code blocks if present
                raw = re.sub(r'```json|```', '', raw).strip()
                p = json.loads(raw)

                d = p.get("dietary", {})
                a = p.get("allergens", {})
                results.append((
                    row["upc"],
                    bool(d.get("is_keto_compliant")),
                    bool(d.get("is_vegan")),
                    bool(d.get("is_gluten_free")),
                    bool(d.get("is_organic")),
                    bool(d.get("is_dairy_free")),
                    bool(d.get("is_paleo")),
                    bool(d.get("is_plant_based")),
                    bool(d.get("is_mediterranean")),
                    bool(a.get("contains_tree_nuts")),
                    bool(a.get("contains_dairy")),
                    bool(a.get("contains_gluten")),
                    bool(a.get("contains_shellfish")),
                    bool(a.get("contains_soy")),
                    bool(a.get("contains_eggs")),
                    str(p.get("margin_segment", "Standard")),
                    bool(p.get("is_pharmacy_product")),
                    p.get("therapeutic_class"),
                    float(p.get("confidence_score", 0.5)),
                    str(p.get("classification_notes", ""))[:500],
                    datetime.utcnow(),
                    ENDPOINT,
                ))
                classified = True
                break

            except Exception as e:
                if attempt < MAX_RETRIES - 1:
                    time.sleep(2 ** attempt)
                    continue

        if not classified:
            # Write error row on complete failure
            results.append((
                row["upc"],
                None, None, None, None, None, None, None, None,
                None, None, None, None, None, None,
                "Standard", False, None, 0.0,
                f"ERROR: classification failed after {MAX_RETRIES} attempts",
                datetime.utcnow(), ENDPOINT,
            ))

    return results

# COMMAND ----------

# Process all SKUs with ThreadPoolExecutor (20 threads, batch size 10)
all_results = []
results_lock = threading.Lock()
completed_count = [0]

# Build batches
batches = []
for i in range(0, len(skus_list), BATCH_SIZE):
    batch = [row.asDict() for row in skus_list[i:i + BATCH_SIZE]]
    batches.append(batch)

num_batches = len(batches)
print(f"Processing {num_batches} batches of {BATCH_SIZE} SKUs with {NUM_THREADS} threads...")

def process_batch(batch):
    batch_results = classify_batch(batch)
    with results_lock:
        all_results.extend(batch_results)
        completed_count[0] += 1
        if completed_count[0] % 100 == 0:
            print(f"  Completed {completed_count[0]}/{num_batches} batches ({len(all_results)} SKUs classified)")
    return len(batch_results)

with ThreadPoolExecutor(max_workers=NUM_THREADS) as executor:
    futures = {executor.submit(process_batch, b): i for i, b in enumerate(batches)}
    for future in as_completed(futures):
        try:
            future.result()
        except Exception as e:
            print(f"  Batch error: {e}")

print(f"\nTotal classified: {len(all_results)}")

# COMMAND ----------

# Write results to v2_ontology.bridge.sku_classifications
classified_df = spark.createDataFrame(all_results, schema=result_schema)

classified_df.write.mode("overwrite") \
    .option("overwriteSchema", "true") \
    .saveAsTable("v2_ontology.bridge.sku_classifications")

total = classified_df.count()
errors = classified_df.filter(F.col("classification_notes").startswith("ERROR")).count()
keto = classified_df.filter(F.col("is_keto_compliant") == True).count()

print(f"Classification complete: {total:,} total, {errors:,} errors, {keto:,} Keto SKUs")
print(f"Success rate: {(total - errors) / total:.1%}")

# COMMAND ----------

# VERIFY: Almond Milk should be keto=TRUE, Oat Milk should be keto=FALSE
print("Keto Verification (Almond Milk vs Oat Milk):")
spark.sql("""
    SELECT s.category_name, s.product_name,
           b.is_keto_compliant, b.is_dairy_free, b.is_vegan, b.confidence_score
    FROM v2_ontology.bridge.sku_classifications b
    JOIN v2_raw.products.sku_master s USING (upc)
    WHERE s.category_name IN ('Almond Milk', 'Oat Milk', 'Coconut Milk', 'Whole Milk')
    ORDER BY s.category_name
""").show(truncate=False)

print("LLM SKU Classification complete!")
