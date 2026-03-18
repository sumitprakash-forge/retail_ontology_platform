# Databricks notebook source
# MAGIC %md
# MAGIC # 03b - Seed Tier 1 Foundation
# MAGIC Seeds the 8 top-level ontology classes and 9 relationship types into v2_ontology.

# COMMAND ----------

from pyspark.sql import Row
from datetime import datetime

now = datetime.utcnow()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Seed 8 Top-Level Classes

# COMMAND ----------

top_classes = [
    ("top_entity",       "Entity",       "Any uniquely identifiable thing"),
    ("top_event",        "Event",        "Something that occurred at a point in time"),
    ("top_location",     "Location",     "A physical or logical place"),
    ("top_time",         "TimeInterval", "A bounded time period"),
    ("top_relationship", "Relationship", "A typed semantic connection between two Entities"),
    ("top_quantity",     "Quantity",     "A measurable amount with units"),
    ("top_role",         "Role",         "How an Entity participates in an Event"),
    ("top_attribute",    "Attribute",    "A property that can be true of an Entity"),
]

classes_df = spark.createDataFrame([
    Row(
        top_class_id=c[0],
        top_class_name=c[1],
        description=c[2],
        is_abstract=True,
        locked_ts=now,
    )
    for c in top_classes
])

classes_df.write.mode("overwrite").saveAsTable("v2_ontology.tier1_foundation.classes_top")
print(f"{len(top_classes)} top-level classes seeded into v2_ontology.tier1_foundation.classes_top")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Seed 9 Relationship Types

# COMMAND ----------

rel_types = [
    ("SUBSTITUTABLE_FOR",       "Can substitute for",            "top_entity",   "top_entity",   True,  False, "product"),
    ("BELONGS_TO_PERSONA",      "Household belongs to persona",  "top_entity",   "top_role",     False, False, "customer"),
    ("FULFILLED_BY",            "Order fulfilled by method",     "top_event",    "top_location", False, False, "supply_chain"),
    ("FREQUENTLY_BOUGHT_WITH",  "Co-occurs in basket",           "top_entity",   "top_entity",   True,  False, "product"),
    ("MEMBER_OF_CLASS",         "Instance belongs to class",     "top_entity",   "top_attribute", False, True,  "all"),
    ("THERAPEUTICALLY_SIMILAR", "Similar therapeutic class",      "top_entity",   "top_entity",   True,  False, "pharmacy"),
    ("ATTRIBUTED_TO_CAMPAIGN",  "Purchase attributed to ad",     "top_event",    "top_event",    False, False, "media"),
    ("LOCATED_IN",              "Entity is at location",         "top_entity",   "top_location", False, False, "supply_chain"),
    ("PRICED_AT",               "Product has price at time",     "top_entity",   "top_quantity", False, False, "finance"),
]

rel_df = spark.createDataFrame([
    Row(
        rel_type_code=r[0],
        rel_type_name=r[1],
        source_top_class=r[2],
        target_top_class=r[3],
        is_symmetric=r[4],
        is_transitive=r[5],
        domain_owner=r[6],
        description=r[1],
    )
    for r in rel_types
])

rel_df.write.mode("overwrite").saveAsTable("v2_ontology.tier1_foundation.relationship_types")
print(f"{len(rel_types)} relationship types seeded into v2_ontology.tier1_foundation.relationship_types")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Verification

# COMMAND ----------

print("=== Tier 1 Foundation Seeded and Locked ===")
print("\nTop-level classes:")
spark.sql("SELECT top_class_id, top_class_name, description FROM v2_ontology.tier1_foundation.classes_top").show(truncate=False)

print("\nRelationship types:")
spark.sql("SELECT rel_type_code, rel_type_name, source_top_class, target_top_class, is_symmetric, is_transitive, domain_owner FROM v2_ontology.tier1_foundation.relationship_types").show(truncate=False)

classes_count = spark.sql("SELECT COUNT(*) AS cnt FROM v2_ontology.tier1_foundation.classes_top").collect()[0].cnt
rels_count = spark.sql("SELECT COUNT(*) AS cnt FROM v2_ontology.tier1_foundation.relationship_types").collect()[0].cnt
print(f"\nCounts: {classes_count} top classes, {rels_count} relationship types")
print("Tier 1 foundation seeded and locked.")
