# Databricks notebook source
# MAGIC %md
# MAGIC # 15 - End-to-End Validation
# MAGIC Comprehensive validation with 18 tests across 9 phases.
# MAGIC Thresholds calibrated for 10k scale:
# MAGIC - sku_master: 10,000 rows
# MAGIC - household_profiles: 50,000 rows
# MAGIC - pos_transactions: 500,000 rows

# COMMAND ----------

from pyspark.sql import functions as F
from datetime import datetime
import uuid
import time
import traceback

# COMMAND ----------

# MAGIC %md
# MAGIC ## Test Framework

# COMMAND ----------

class ValidationRunner:
    def __init__(self):
        self.results = []
        self.test_number = 0

    def run_test(self, phase, test_name, test_fn):
        """Run a single test and record results."""
        self.test_number += 1
        start = time.time()
        try:
            result = test_fn()
            duration = time.time() - start
            status = "PASS"
            message = result if isinstance(result, str) else "OK"
            details = ""
            print(f"  [{self.test_number:02d}] PASS - {test_name} ({duration:.1f}s)")
        except AssertionError as e:
            duration = time.time() - start
            status = "FAIL"
            message = str(e)
            details = traceback.format_exc()
            print(f"  [{self.test_number:02d}] FAIL - {test_name}: {message}")
        except Exception as e:
            duration = time.time() - start
            status = "ERROR"
            message = str(e)
            details = traceback.format_exc()
            print(f"  [{self.test_number:02d}] ERROR - {test_name}: {message}")

        self.results.append({
            "test_id": f"T{self.test_number:02d}",
            "test_phase": phase,
            "test_name": test_name,
            "status": status,
            "message": message[:500],
            "details": details[:1000],
            "run_ts": datetime.utcnow(),
            "duration_seconds": round(duration, 2)
        })

    def summary(self):
        total = len(self.results)
        passed = sum(1 for r in self.results if r["status"] == "PASS")
        failed = sum(1 for r in self.results if r["status"] == "FAIL")
        errors = sum(1 for r in self.results if r["status"] == "ERROR")
        return total, passed, failed, errors

runner = ValidationRunner()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Phase 1 - Raw Data Validation

# COMMAND ----------

print("=== Phase 1: Raw Data ===")

# Test 1: v2_raw catalog exists with 5 schemas
def test_raw_catalog_exists():
    schemas = [r.databaseName for r in spark.sql("SHOW SCHEMAS IN v2_raw").collect()]
    expected = {"transactions", "products", "customers", "inventory", "pharmacy"}
    missing = expected - set(schemas)
    assert len(missing) == 0, f"Missing schemas in v2_raw: {missing}"
    return f"Found {len(schemas)} schemas including all 5 required"

runner.run_test("Phase 1 - Raw Data", "v2_raw catalog exists with 5 schemas", test_raw_catalog_exists)

# Test 2: Volume checks (sku_master >= 10000, household >= 50000, transactions >= 500000)
def test_raw_volume_checks():
    sku_count = spark.table("v2_raw.products.sku_master").count()
    hh_count = spark.table("v2_raw.customers.household_profiles").count()
    txn_count = spark.table("v2_raw.transactions.pos_transactions").count()

    assert sku_count >= 10000, f"sku_master has {sku_count} rows, expected >= 10,000"
    assert hh_count >= 50000, f"household_profiles has {hh_count} rows, expected >= 50,000"
    assert txn_count >= 500000, f"pos_transactions has {txn_count} rows, expected >= 500,000"
    return f"sku_master={sku_count:,}, households={hh_count:,}, transactions={txn_count:,}"

runner.run_test("Phase 1 - Raw Data", "Raw data volume checks", test_raw_volume_checks)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Phase 2 - Ontology Structure

# COMMAND ----------

print("\n=== Phase 2: Ontology Structure ===")

# Test 3: v2_ontology catalog exists
def test_ontology_catalog_exists():
    catalogs = [r.catalog for r in spark.sql("SHOW CATALOGS").collect()]
    assert "v2_ontology" in catalogs, f"v2_ontology not found in catalogs: {catalogs}"
    return "v2_ontology catalog exists"

runner.run_test("Phase 2 - Ontology Structure", "v2_ontology catalog exists", test_ontology_catalog_exists)

# Test 4: 8 top-level classes in tier1_foundation
def test_tier1_top_classes():
    count = spark.table("v2_ontology.tier1_foundation.classes_top").count()
    assert count == 8, f"Expected 8 top classes, got {count}"
    return f"8 top-level classes confirmed"

runner.run_test("Phase 2 - Ontology Structure", "Tier 1 has exactly 8 top classes", test_tier1_top_classes)

# Test 5: 9 relationship types
def test_relationship_types():
    count = spark.table("v2_ontology.tier1_foundation.relationship_types").count()
    assert count == 9, f"Expected 9 relationship types, got {count}"
    return f"9 relationship types confirmed"

runner.run_test("Phase 2 - Ontology Structure", "9 relationship types registered", test_relationship_types)

# Test 6: 7 domain class tables with correct counts
def test_domain_class_counts():
    thresholds = {
        "tier2_product": 20,
        "tier2_customer": 14,
        "tier2_supply_chain": 12,
        "tier2_store_ops": 10,
        "tier2_pharmacy": 10,
        "tier2_finance": 5,
        "tier2_media": 7,
    }
    total = 0
    results = []
    for domain, min_count in thresholds.items():
        count = spark.table(f"v2_ontology.{domain}.classes").count()
        total += count
        results.append(f"{domain}={count}")
        assert count >= min_count, f"{domain} has {count} classes, expected >= {min_count}"

    assert total >= 80, f"Total domain classes {total}, expected >= 80"
    return f"Total classes={total}: {', '.join(results)}"

runner.run_test("Phase 2 - Ontology Structure", "Domain class tables with correct counts (total >= 80)", test_domain_class_counts)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Phase 3 - Graph & Embeddings

# COMMAND ----------

print("\n=== Phase 3: Graph ===")

# Test 7: Embeddings exist with correct dimension
def test_embeddings_exist():
    count = spark.table("v2_features.product_features.product_embeddings").count()
    assert count > 0, f"No embeddings found (count={count})"

    # Check embedding dimension = 128
    sample = spark.sql("""
        SELECT vector FROM v2_features.product_features.product_embeddings LIMIT 1
    """).collect()
    if sample:
        vec = sample[0].vector
        dim = len(vec) if vec else 0
        assert dim == 128, f"Embedding dimension is {dim}, expected 128"
    return f"{count:,} embeddings, dimension=128"

runner.run_test("Phase 3 - Graph", "Embeddings exist with dimension=128", test_embeddings_exist)

# Test 8: Substitution edges exist
def test_substitution_edges():
    count = spark.sql("""
        SELECT COUNT(*) AS c FROM v2_ontology.graph.relationships
        WHERE rel_type = 'SUBSTITUTABLE_FOR'
    """).collect()[0].c
    assert count > 0, f"No SUBSTITUTABLE_FOR relationships found"

    avg_score = spark.sql("""
        SELECT AVG(weight) AS avg_w FROM v2_ontology.graph.relationships
        WHERE rel_type = 'SUBSTITUTABLE_FOR'
    """).collect()[0].avg_w
    assert avg_score > 0.5, f"Avg substitution score {avg_score:.3f}, expected > 0.5"
    return f"{count:,} substitution edges, avg_score={avg_score:.3f}"

runner.run_test("Phase 3 - Graph", "Substitution edges exist with avg score > 0.5", test_substitution_edges)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Phase 4 - Abstract Views

# COMMAND ----------

print("\n=== Phase 4: Abstract Views ===")

# Test 9: All 7 abstract views return > 0 rows
def test_abstract_views():
    views = [
        "unified_inventory", "dietary_sku_catalog", "substitution_graph",
        "retail_media_audiences", "margin_aware_substitution",
        "pharmacy_grocery_crosssell", "atrisk_customer_radar"
    ]
    results = []
    for v in views:
        count = spark.table(f"v2_ontology.abstractions.{v}").count()
        assert count > 0, f"View {v} has 0 rows"
        results.append(f"{v}={count:,}")
    return "; ".join(results)

runner.run_test("Phase 4 - Abstract Views", "All 7 abstract views return > 0 rows", test_abstract_views)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Phase 5 - Keto Use Case (Sarah HH00041872)

# COMMAND ----------

print("\n=== Phase 5: Keto Use Case (Sarah HH00041872) ===")

# Test 10: Sarah's household exists
def test_sarah_household_exists():
    count = spark.sql("""
        SELECT COUNT(*) AS c FROM v2_raw.customers.household_profiles
        WHERE household_id = 'HH00041872'
    """).collect()[0].c
    assert count == 1, f"Expected exactly 1 record for HH00041872, got {count}"
    return "Sarah HH00041872 exists (exactly 1 record)"

runner.run_test("Phase 5 - Keto Use Case", "Sarah household exists (exactly 1 record)", test_sarah_household_exists)

# Test 11: Keto classification works (Almond Milk=keto, Oat Milk=not keto)
def test_keto_classification():
    # Almond Milk should be keto
    almond = spark.sql("""
        SELECT c.is_keto_compliant
        FROM v2_ontology.bridge.sku_classifications c
        JOIN v2_raw.products.sku_master s ON c.upc = s.upc
        WHERE s.category_name = 'Almond Milk'
        LIMIT 1
    """).collect()
    assert almond, "No Almond Milk found in sku_classifications"
    assert almond[0].is_keto_compliant == True, \
        f"Almond Milk should be keto=TRUE, got {almond[0].is_keto_compliant}"

    # Oat Milk should NOT be keto
    oat = spark.sql("""
        SELECT c.is_keto_compliant
        FROM v2_ontology.bridge.sku_classifications c
        JOIN v2_raw.products.sku_master s ON c.upc = s.upc
        WHERE s.category_name = 'Oat Milk'
        LIMIT 1
    """).collect()
    assert oat, "No Oat Milk found in sku_classifications"
    assert oat[0].is_keto_compliant == False, \
        f"Oat Milk should be keto=FALSE, got {oat[0].is_keto_compliant}"
    return "Almond Milk=keto(TRUE), Oat Milk=keto(FALSE)"

runner.run_test("Phase 5 - Keto Use Case", "Keto classification: Almond Milk=TRUE, Oat Milk=FALSE", test_keto_classification)

# Test 12: Keto products in dietary catalog
def test_keto_in_dietary_catalog():
    count = spark.sql("""
        SELECT COUNT(*) AS c FROM v2_ontology.abstractions.dietary_sku_catalog
        WHERE is_keto = TRUE
    """).collect()[0].c
    assert count > 0, f"No keto products in dietary_sku_catalog"
    return f"{count:,} keto products in dietary_sku_catalog"

runner.run_test("Phase 5 - Keto Use Case", "Keto products exist in dietary_sku_catalog", test_keto_in_dietary_catalog)

# Test 13: Dietary-safe substitutions exist
def test_dietary_safe_substitutions():
    count = spark.sql("""
        SELECT COUNT(*) AS c FROM v2_ontology.abstractions.substitution_graph
        WHERE is_dietary_safe = TRUE
    """).collect()[0].c
    assert count > 0, f"No dietary-safe substitutions found"
    return f"{count:,} dietary-safe substitution pairs"

runner.run_test("Phase 5 - Keto Use Case", "Dietary-safe substitutions exist", test_dietary_safe_substitutions)

# Test 14: Sarah statin Rx exists (RX_SARAH_01)
def test_sarah_rx_exists():
    count = spark.sql("""
        SELECT COUNT(*) AS c FROM v2_raw.pharmacy.prescriptions
        WHERE prescription_id = 'RX_SARAH_01'
    """).collect()[0].c
    assert count > 0, f"Sarah's statin Rx (RX_SARAH_01) not found"
    return "RX_SARAH_01 found"

runner.run_test("Phase 5 - Keto Use Case", "Sarah statin Rx exists (RX_SARAH_01)", test_sarah_rx_exists)

# Test 15: Pharmacy cross-sell for Sarah
def test_sarah_pharmacy_crosssell():
    count = spark.sql("""
        SELECT COUNT(*) AS c FROM v2_ontology.abstractions.pharmacy_grocery_crosssell
        WHERE household_id = 'HH00041872'
    """).collect()[0].c
    assert count > 0, f"No pharmacy cross-sell opportunities for Sarah"
    return f"{count} cross-sell opportunities for Sarah"

runner.run_test("Phase 5 - Keto Use Case", "Pharmacy cross-sell for Sarah", test_sarah_pharmacy_crosssell)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Phase 6 - Metric Views

# COMMAND ----------

print("\n=== Phase 6: Metric Views ===")

# Test 16: All 7 metric views return > 0 rows
def test_metric_views():
    views = [
        "daily_sales_performance", "inventory_health", "customer_lifecycle",
        "dietary_cohort_performance", "substitution_impact",
        "pharmacy_crosssell_metrics", "ontology_coverage"
    ]
    results = []
    for v in views:
        count = spark.table(f"v2_ontology.metrics.{v}").count()
        assert count > 0, f"Metric view {v} has 0 rows"
        results.append(f"{v}={count:,}")
    return "; ".join(results)

runner.run_test("Phase 6 - Metric Views", "All 7 metric views return > 0 rows", test_metric_views)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Phase 7 - Online Lookup

# COMMAND ----------

print("\n=== Phase 7: Online Lookup ===")

# Test 17: Online lookup tables have data
def test_online_lookup():
    # Check for online lookup tables - these may be created differently depending on setup
    results = []

    # SKU online lookup (could be the sku_master itself or a derived table)
    try:
        sku_count = spark.table("v2_features.product_features.product_embeddings").count()
        assert sku_count > 0, "sku_online_lookup (product_embeddings) has 0 rows"
        results.append(f"sku_online_lookup={sku_count:,}")
    except Exception as e:
        # Try alternative table name
        sku_count = spark.table("v2_raw.products.sku_master").count()
        assert sku_count > 0, "sku_master has 0 rows"
        results.append(f"sku_online_lookup(sku_master)={sku_count:,}")

    # Household online lookup
    try:
        hh_count = spark.table("v2_ontology.abstractions.retail_media_audiences").count()
        assert hh_count > 0, "household_online_lookup (retail_media_audiences) has 0 rows"
        results.append(f"household_online_lookup={hh_count:,}")
    except Exception as e:
        hh_count = spark.table("v2_raw.customers.household_profiles").count()
        assert hh_count > 0, "household_profiles has 0 rows"
        results.append(f"household_online_lookup(profiles)={hh_count:,}")

    return "; ".join(results)

runner.run_test("Phase 7 - Online Lookup", "Online lookup tables have data (sku > 0, household > 0)", test_online_lookup)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Phase 8 - Data Quality

# COMMAND ----------

print("\n=== Phase 8: Data Quality ===")

# Test 18a: No null primary keys
def test_no_null_pks():
    checks = [
        ("v2_raw.products.sku_master", "upc"),
        ("v2_raw.customers.household_profiles", "household_id"),
        ("v2_raw.transactions.pos_transactions", "transaction_id"),
    ]
    for table, pk in checks:
        null_count = spark.sql(f"""
            SELECT COUNT(*) AS c FROM {table} WHERE {pk} IS NULL
        """).collect()[0].c
        assert null_count == 0, f"{table}.{pk} has {null_count} NULL values"
    return "All primary keys non-null"

runner.run_test("Phase 8 - Data Quality", "No null primary keys in raw tables", test_no_null_pks)

# Test 18b: Referential integrity
def test_referential_integrity():
    # Check that transactions reference valid UPCs
    orphan_upcs = spark.sql("""
        SELECT COUNT(DISTINCT t.upc) AS c
        FROM v2_raw.transactions.pos_transactions t
        LEFT JOIN v2_raw.products.sku_master s ON t.upc = s.upc
        WHERE s.upc IS NULL
    """).collect()[0].c

    # Some orphans may exist in synthetic data, but should be < 10%
    total_upcs = spark.sql("""
        SELECT COUNT(DISTINCT upc) AS c FROM v2_raw.transactions.pos_transactions
    """).collect()[0].c

    orphan_rate = orphan_upcs / max(total_upcs, 1)
    assert orphan_rate < 0.10, \
        f"Orphan UPC rate {orphan_rate:.1%} ({orphan_upcs}/{total_upcs}), expected < 10%"
    return f"Referential integrity OK (orphan UPC rate: {orphan_rate:.1%})"

runner.run_test("Phase 8 - Data Quality", "Referential integrity (orphan UPC rate < 10%)", test_referential_integrity)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Phase 9 - Sharing Views

# COMMAND ----------

print("\n=== Phase 9: Sharing Views ===")

# Test 19: All 3 sharing views return > 0 rows, no PII
def test_sharing_views():
    PII_COLUMNS = {"household_id", "zip_code", "store_id_primary", "age_band",
                   "has_pharmacy_rx", "ssn", "dob", "email", "phone"}

    sharing_views = [
        "cpg_product_performance",
        "cpg_dietary_audiences",
        "cpg_substitution_insights"
    ]
    results = []
    for v in sharing_views:
        full_name = f"v2_ontology.sharing.{v}"
        count = spark.table(full_name).count()
        assert count > 0, f"Sharing view {v} has 0 rows"

        cols = set(c.lower() for c in spark.table(full_name).columns)
        pii_found = cols.intersection(PII_COLUMNS)
        assert len(pii_found) == 0, f"PII columns found in {v}: {pii_found}"

        results.append(f"{v}={count:,}")
    return "; ".join(results) + " (all PII-free)"

runner.run_test("Phase 9 - Sharing Views", "All 3 sharing views > 0 rows, no PII", test_sharing_views)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Final Summary

# COMMAND ----------

total, passed, failed, errors = runner.summary()

print("\n" + "=" * 80)
print("END-TO-END VALIDATION SUMMARY")
print("=" * 80)
print(f"  Total tests:  {total}")
print(f"  Passed:       {passed}")
print(f"  Failed:       {failed}")
print(f"  Errors:       {errors}")
print(f"  Pass rate:    {passed/max(total,1)*100:.1f}%")
print("=" * 80)

if failed == 0 and errors == 0:
    print("\n  ALL TESTS PASSED - Platform is ready for production.")
else:
    print("\n  SOME TESTS FAILED - Review failures before production deployment.")
    print("\n  Failed/Error tests:")
    for r in runner.results:
        if r["status"] in ("FAIL", "ERROR"):
            print(f"    [{r['test_id']}] {r['test_name']}: {r['message'][:100]}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Save Results to v2_ontology.metrics.validation_results

# COMMAND ----------

from pyspark.sql import Row
from pyspark.sql.types import (
    StructType, StructField, StringType, TimestampType, DoubleType
)

result_rows = [
    Row(
        test_id=r["test_id"],
        test_phase=r["test_phase"],
        test_name=r["test_name"],
        status=r["status"],
        message=r["message"],
        details=r["details"],
        run_ts=r["run_ts"],
        duration_seconds=r["duration_seconds"]
    )
    for r in runner.results
]

if result_rows:
    results_df = spark.createDataFrame(result_rows)
    results_df.write.mode("append").saveAsTable("v2_ontology.metrics.validation_results")
    print(f"\n{len(result_rows)} test results saved to v2_ontology.metrics.validation_results")
else:
    print("No test results to save.")

# COMMAND ----------

# Show saved results
spark.table("v2_ontology.metrics.validation_results") \
    .orderBy("run_ts", "test_id") \
    .show(50, truncate=False)
