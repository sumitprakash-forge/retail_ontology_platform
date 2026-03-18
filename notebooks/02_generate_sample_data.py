# Databricks notebook source
# MAGIC %md
# MAGIC # 02 - Generate Sample Data
# MAGIC Generates synthetic retail data: 10K SKUs, 50K households, 500K transactions.
# MAGIC Includes the Keto dietary use case (Sarah, HH00041872).

# COMMAND ----------

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import *
import random
from datetime import datetime, timedelta
import uuid

spark = SparkSession.builder.getOrCreate()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Constants

# COMMAND ----------

N_SKUS         = 10_000
N_HOUSEHOLDS   = 50_000
N_STORES       = 500
N_TRANSACTIONS = 500_000

# SKU profiles -- dietary ground truth for testing Keto example end-to-end
SKU_PROFILES = [
    # (subcategory, dietary_flags, dept_code, certifications)
    # KETO-FRIENDLY
    ("Almond Milk",     {"keto": True,  "vegan": True,  "gf": True,  "dairy_free": True},  "07", "KETO|VEGAN|GF|DAIRY_FREE"),
    ("Coconut Milk",    {"keto": True,  "vegan": True,  "gf": True,  "dairy_free": True},  "07", "KETO|VEGAN|GF|DAIRY_FREE"),
    ("Heavy Cream",     {"keto": True,  "vegan": False, "gf": True,  "dairy_free": False}, "03", "KETO|GF"),
    ("Chicken Breast",  {"keto": True,  "vegan": False, "gf": True,  "dairy_free": True},  "02", "KETO|GF|DAIRY_FREE"),
    ("Cauliflower",     {"keto": True,  "vegan": True,  "gf": True,  "dairy_free": True},  "01", "KETO|VEGAN|GF|DAIRY_FREE"),
    ("Avocado",         {"keto": True,  "vegan": True,  "gf": True,  "dairy_free": True},  "01", "KETO|VEGAN|GF|DAIRY_FREE"),
    ("Cheddar Cheese",  {"keto": True,  "vegan": False, "gf": True,  "dairy_free": False}, "03", "KETO|GF"),
    ("Bacon",           {"keto": True,  "vegan": False, "gf": True,  "dairy_free": True},  "02", "KETO|GF|DAIRY_FREE"),
    ("Salmon Fillet",   {"keto": True,  "vegan": False, "gf": True,  "dairy_free": True},  "02", "KETO|GF|DAIRY_FREE"),
    ("Spinach",         {"keto": True,  "vegan": True,  "gf": True,  "dairy_free": True},  "01", "KETO|VEGAN|GF|DAIRY_FREE"),
    # NOT KETO
    ("Oat Milk",        {"keto": False, "vegan": True,  "gf": True,  "dairy_free": True},  "07", "VEGAN|GF|DAIRY_FREE"),
    ("Whole Milk",      {"keto": True,  "vegan": False, "gf": True,  "dairy_free": False}, "03", "KETO|GF"),
    ("White Bread",     {"keto": False, "vegan": True,  "gf": False, "dairy_free": True},  "04", "VEGAN|DAIRY_FREE"),
    ("Pasta",           {"keto": False, "vegan": True,  "gf": False, "dairy_free": True},  "04", "VEGAN|DAIRY_FREE"),
    ("White Rice",      {"keto": False, "vegan": True,  "gf": True,  "dairy_free": True},  "04", "VEGAN|GF|DAIRY_FREE"),
    ("Potato Chips",    {"keto": False, "vegan": True,  "gf": True,  "dairy_free": True},  "08", "VEGAN|GF|DAIRY_FREE"),
    ("Orange Juice",    {"keto": False, "vegan": True,  "gf": True,  "dairy_free": True},  "07", "VEGAN|GF|DAIRY_FREE"),
    ("Yogurt",          {"keto": False, "vegan": False, "gf": True,  "dairy_free": False}, "03", "GF"),
    ("Granola Bar",     {"keto": False, "vegan": True,  "gf": False, "dairy_free": True},  "08", "VEGAN|DAIRY_FREE"),
    ("Cookies",         {"keto": False, "vegan": False, "gf": False, "dairy_free": False}, "08", None),
]

BRANDS = [
    "StoreBrand", "Simple Truth", "Private Selection", "Chobani", "Kellogg's",
    "General Mills", "Nestle", "PepsiCo", "Kraft Heinz", "Unilever",
    "Clif Bar", "Amy's Kitchen", "Horizon Organic", "Bob's Red Mill",
]

DEPT_NAME_MAP = {
    "01": "Produce", "02": "Meat", "03": "Dairy", "04": "Bakery",
    "07": "Beverages", "08": "Snacks",
}

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. Generate SKU Master (10,000 rows)

# COMMAND ----------

def generate_skus(n=N_SKUS):
    rows = []
    for i in range(n):
        profile_key, flags, dept_code, certs = random.choice(SKU_PROFILES)
        brand = random.choice(BRANDS)
        rows.append((
            f"{random.randint(10000000000, 99999999999):011d}",
            f"{brand} {profile_key} {i}",
            brand,
            None,  # manufacturer
            dept_code,
            DEPT_NAME_MAP.get(dept_code, "General"),
            f"CAT_{dept_code}_{random.randint(1, 10):02d}",
            profile_key,
            f"subcat_{random.randint(1, 5)}",
            "EA",
            f"Contains: {profile_key.lower()}. May contain nuts.",
            certs or None,
            brand in ["StoreBrand", "Simple Truth", "Private Selection"],
            dept_code in ["01", "02", "03"],
            round(random.uniform(0.99, 24.99), 2),
            datetime.now().date(),
        ))
    schema = StructType([
        StructField("upc", StringType()),
        StructField("product_name", StringType()),
        StructField("brand", StringType()),
        StructField("manufacturer", StringType()),
        StructField("department_code", StringType()),
        StructField("department_name", StringType()),
        StructField("category_code", StringType()),
        StructField("category_name", StringType()),
        StructField("subcategory_code", StringType()),
        StructField("unit_of_measure", StringType()),
        StructField("ingredients_text", StringType()),
        StructField("certifications", StringType()),
        StructField("is_private_label", BooleanType()),
        StructField("is_perishable", BooleanType()),
        StructField("avg_retail_price", DoubleType()),
        StructField("effective_date", DateType()),
    ])
    return spark.createDataFrame(rows, schema)

skus_df = generate_skus()
skus_df.withColumn("avg_retail_price", F.col("avg_retail_price").cast("decimal(10,2)")) \
    .write.mode("overwrite").option("overwriteSchema", "true").saveAsTable("v2_raw.products.sku_master")
print(f"{N_SKUS:,} SKUs written to v2_raw.products.sku_master")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. Generate Household Profiles (50,000 rows)

# COMMAND ----------

# Sarah -- deterministic for testing
sarah_row = [(
    "HH00041872", "45202", "STR0042", "PLUS", 2,
    datetime(2019, 3, 15).date(), True, "35-44", False, False
)]

# Bulk households
hh_rows = sarah_row + [(
    f"HH{i:08d}",
    f"{random.randint(10000, 99999)}",
    f"STR{random.randint(0, N_STORES):04d}",
    random.choice(["STANDARD", "GOLD", "PLUS"]),
    random.randint(1, 5),
    datetime(2015 + random.randint(0, 8), random.randint(1, 12), 1).date(),
    True,
    random.choice(["25-34", "35-44", "45-54", "55-64"]),
    random.random() < 0.15,
    random.random() < 0.30,
) for i in range(1, N_HOUSEHOLDS)]

hh_schema = StructType([
    StructField("household_id", StringType()),
    StructField("zip_code", StringType()),
    StructField("store_id_primary", StringType()),
    StructField("loyalty_tier", StringType()),
    StructField("household_size", IntegerType()),
    StructField("enrollment_date", DateType()),
    StructField("is_active", BooleanType()),
    StructField("age_band", StringType()),
    StructField("has_pharmacy_rx", BooleanType()),
    StructField("has_fuel_rewards", BooleanType()),
])

spark.createDataFrame(hh_rows, hh_schema) \
    .write.mode("overwrite").saveAsTable("v2_raw.customers.household_profiles")
print(f"{N_HOUSEHOLDS:,} households written (including Sarah HH00041872)")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. Generate Transactions (500,000 bulk + 200 Sarah)

# COMMAND ----------

# Collect UPC lists for transaction generation
upcs_all = [r.upc for r in skus_df.select("upc").collect()]
keto_upcs = [r.upc for r in skus_df.filter(
    F.col("certifications").contains("KETO")
).select("upc").collect()] or upcs_all[:500]

base_date = datetime.now() - timedelta(days=90)
channels = ["IN_STORE"] * 60 + ["PICKUP"] * 25 + ["DELIVERY"] * 10 + ["SHIP"] * 5

# COMMAND ----------

# Sarah's Keto shopping pattern (200 transactions, 60% keto UPCs)
sarah_txn_rows = []
for i in range(200):
    ts = base_date + timedelta(days=random.randint(0, 90), hours=random.randint(7, 21))
    upc = random.choice(keto_upcs if random.random() < 0.6 else upcs_all)
    sarah_txn_rows.append((
        str(uuid.uuid4()), "HH00041872", "STR0042", upc,
        random.randint(1, 3), round(random.uniform(2.99, 18.99), 2),
        round(random.uniform(1.50, 10.00), 4), 0.0, ts, "PICKUP", "STR0042",
    ))

sarah_txn_schema = StructType([
    StructField("transaction_id", StringType()),
    StructField("household_id", StringType()),
    StructField("store_id", StringType()),
    StructField("upc", StringType()),
    StructField("quantity", IntegerType()),
    StructField("unit_price", DoubleType()),
    StructField("unit_cost", DoubleType()),
    StructField("discount_amt", DoubleType()),
    StructField("transaction_ts", TimestampType()),
    StructField("channel", StringType()),
    StructField("fulfillment_node_id", StringType()),
])

sarah_df = spark.createDataFrame(sarah_txn_rows, sarah_txn_schema)
print(f"Sarah's {len(sarah_txn_rows)} transactions prepared")

# COMMAND ----------

# Bulk transactions — pure Python generation (sparkContext not supported on serverless)
print(f"Generating {N_TRANSACTIONS:,} bulk transactions in Python...")
bulk_txn_rows = []
for i in range(N_TRANSACTIONS):
    random.seed(i + 42)
    ts = base_date + timedelta(seconds=random.randint(0, 90 * 86400))
    hh_id = f"HH{random.randint(0, N_HOUSEHOLDS):08d}" if random.random() > 0.15 else None
    store = f"STR{random.randint(0, N_STORES):04d}"
    bulk_txn_rows.append((
        str(uuid.uuid4()),
        hh_id,
        store,
        random.choice(upcs_all),
        random.randint(1, 5),
        round(random.uniform(0.99, 24.99), 2),
        round(random.uniform(0.50, 15.00), 4),
        round(random.uniform(0, 2.00), 2),
        ts,
        random.choice(channels),
        store,
    ))

bulk_df = spark.createDataFrame(bulk_txn_rows, sarah_txn_schema)
print(f"Bulk DataFrame created: {len(bulk_txn_rows):,} rows")

# Union Sarah + bulk and write (cast Double -> Decimal to match table schema)
def cast_txn(df):
    return df.withColumn("unit_price",   F.col("unit_price").cast("decimal(10,2)")) \
             .withColumn("unit_cost",    F.col("unit_cost").cast("decimal(10,4)")) \
             .withColumn("discount_amt", F.col("discount_amt").cast("decimal(10,2)"))

all_txn_df = cast_txn(sarah_df).unionByName(cast_txn(bulk_df))
all_txn_df.write.mode("overwrite").option("overwriteSchema", "true").saveAsTable("v2_raw.transactions.pos_transactions")
print(f"{N_TRANSACTIONS + 200:,} transactions written (200 Sarah + {N_TRANSACTIONS:,} bulk)")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4. Generate Node Inventory Snapshots (50,000 rows)

# COMMAND ----------

N_INVENTORY = 50_000

# Inventory snapshots — pure Python generation (sparkContext not supported on serverless)
print(f"Generating {N_INVENTORY:,} inventory snapshots in Python...")
node_types_list = ["STORE", "DC_REGIONAL", "DC_FULFILLMENT", "3P_INST"]
inv_rows = []
for i in range(N_INVENTORY):
    random.seed(i + 999999)
    node_type = random.choice(node_types_list)
    node_id = f"STR{random.randint(0, N_STORES):04d}" if node_type == "STORE" else f"DC{random.randint(1, 20):03d}"
    ts = datetime.now() - timedelta(hours=random.randint(0, 720))
    inv_rows.append((
        str(uuid.uuid4()),
        node_id,
        node_type,
        random.choice(upcs_all),
        random.randint(0, 500),
        random.randint(0, 50),
        random.randint(0, 100),
        random.randint(10, 100),
        ts,
    ))

inv_schema = StructType([
    StructField("snapshot_id",    StringType()),
    StructField("node_id",        StringType()),
    StructField("node_type",      StringType()),
    StructField("upc",            StringType()),
    StructField("on_hand_qty",    IntegerType()),
    StructField("reserved_qty",   IntegerType()),
    StructField("in_transit_qty", IntegerType()),
    StructField("reorder_point",  IntegerType()),
    StructField("snapshot_ts",    TimestampType()),
])
inv_df = spark.createDataFrame(inv_rows, inv_schema)

inv_df.write.mode("overwrite").saveAsTable("v2_raw.inventory.node_inventory_snapshots")
print(f"{N_INVENTORY:,} inventory snapshots written")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 5. Generate Pharmacy Prescriptions (1,000 rows + Sarah's statin Rx)

# COMMAND ----------

DRUG_CLASSES = ["Statins", "ACE Inhibitors", "Beta Blockers", "SSRIs", "Metformin",
                "Proton Pump Inhibitors", "Antihistamines", "NSAIDs"]
DRUG_NAMES = {
    "Statins": "Atorvastatin", "ACE Inhibitors": "Lisinopril",
    "Beta Blockers": "Metoprolol", "SSRIs": "Sertraline",
    "Metformin": "Metformin HCl", "Proton Pump Inhibitors": "Omeprazole",
    "Antihistamines": "Cetirizine", "NSAIDs": "Ibuprofen",
}

# Sarah's statin prescription
sarah_rx = [(
    "RX_SARAH_01", "HH00041872", "STR0042", "00071015523",
    "Atorvastatin", "Statins",
    datetime(2025, 11, 1).date(), 90, 30.0, 2, "DR00421", 15.00,
)]

# Bulk prescriptions
rx_rows = sarah_rx + [(
    f"RX{i:08d}",
    f"HH{random.randint(0, N_HOUSEHOLDS):08d}",
    f"STR{random.randint(0, N_STORES):04d}",
    f"{random.randint(10000000000, 99999999999):011d}",
    DRUG_NAMES.get(dc := random.choice(DRUG_CLASSES), dc),
    dc,
    (datetime.now() - timedelta(days=random.randint(0, 365))).date(),
    random.choice([30, 60, 90]),
    round(random.uniform(10.0, 120.0), 2),
    random.randint(0, 12),
    f"DR{random.randint(100, 999):05d}",
    round(random.uniform(5.0, 75.0), 2),
) for i in range(1, 1000)]

rx_schema = StructType([
    StructField("rx_id", StringType()),
    StructField("household_id", StringType()),
    StructField("store_id", StringType()),
    StructField("ndc_code", StringType()),
    StructField("drug_name", StringType()),
    StructField("drug_class", StringType()),
    StructField("fill_date", DateType()),
    StructField("days_supply", IntegerType()),
    StructField("quantity", DoubleType()),
    StructField("refill_number", IntegerType()),
    StructField("prescriber_id", StringType()),
    StructField("copay_amount", DoubleType()),
])

spark.createDataFrame(rx_rows, rx_schema) \
    .withColumn("quantity",     F.col("quantity").cast("decimal(10,2)")) \
    .withColumn("copay_amount", F.col("copay_amount").cast("decimal(10,2)")) \
    .write.mode("overwrite").option("overwriteSchema", "true").saveAsTable("v2_raw.pharmacy.prescriptions")
print(f"{len(rx_rows):,} prescriptions written (including Sarah's statin Rx)")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 6. Generate Drug Interactions (8 rows)

# COMMAND ----------

drug_interactions = [
    ("Statins", "SSRIs", "Moderate", "Increased risk of myopathy when combined", True, "Avoid grapefruit juice -- inhibits CYP3A4 metabolism"),
    ("Statins", "Metformin", "Low", "Generally safe combination for diabetic patients", True, "High-fiber meals may reduce absorption of both drugs"),
    ("ACE Inhibitors", "NSAIDs", "High", "NSAIDs reduce antihypertensive effect and increase renal risk", True, "High-sodium foods counteract ACE inhibitor effect"),
    ("ACE Inhibitors", "Metformin", "Moderate", "May increase metformin levels; monitor renal function", False, None),
    ("Beta Blockers", "SSRIs", "Moderate", "SSRIs may increase beta blocker plasma levels via CYP2D6", False, None),
    ("Beta Blockers", "NSAIDs", "Moderate", "NSAIDs may reduce antihypertensive effect of beta blockers", True, "Caffeine may counteract blood pressure lowering effect"),
    ("Metformin", "Proton Pump Inhibitors", "Low", "PPIs may reduce vitamin B12 absorption; monitor with long-term metformin", True, "Take metformin with food to reduce GI side effects"),
    ("SSRIs", "NSAIDs", "High", "Increased risk of GI bleeding when combined", True, "Alcohol significantly increases GI bleeding risk"),
]

di_schema = StructType([
    StructField("drug_class_1", StringType()),
    StructField("drug_class_2", StringType()),
    StructField("interaction_severity", StringType()),
    StructField("interaction_description", StringType()),
    StructField("food_interaction_flag", BooleanType()),
    StructField("food_interaction_detail", StringType()),
])

spark.createDataFrame(drug_interactions, di_schema) \
    .write.mode("overwrite").saveAsTable("v2_raw.pharmacy.drug_interactions")
print(f"{len(drug_interactions)} drug interactions written")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 7. Verification

# COMMAND ----------

print("=== Sarah's Data ===")
print("\nHousehold profile:")
spark.sql("SELECT * FROM v2_raw.customers.household_profiles WHERE household_id = 'HH00041872'").show(truncate=False)

print("\nSarah's transaction count:")
spark.sql("SELECT COUNT(*) AS sarah_txn_count FROM v2_raw.transactions.pos_transactions WHERE household_id = 'HH00041872'").show()

print("\nSarah's prescription:")
spark.sql("SELECT * FROM v2_raw.pharmacy.prescriptions WHERE rx_id = 'RX_SARAH_01'").show(truncate=False)

# COMMAND ----------

print("=== Table Counts ===")
tables = [
    "v2_raw.products.sku_master",
    "v2_raw.customers.household_profiles",
    "v2_raw.transactions.pos_transactions",
    "v2_raw.inventory.node_inventory_snapshots",
    "v2_raw.pharmacy.prescriptions",
    "v2_raw.pharmacy.drug_interactions",
]
for t in tables:
    count = spark.sql(f"SELECT COUNT(*) AS cnt FROM {t}").collect()[0].cnt
    print(f"  {t}: {count:,}")

print("\nSample data generation complete.")
