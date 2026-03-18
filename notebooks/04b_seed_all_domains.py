# Databricks notebook source
# 04b - Seed All Domain Classes (~92 Classes Across 7 Domains)
# Seeds domain-level ontology classes per spec Section 6.3.
# Writes to v2_ontology.tier2_*.classes tables with mode("overwrite").

# COMMAND ----------

from pyspark.sql import Row
from pyspark.sql import functions as F
from pyspark.sql.types import *
from datetime import datetime

now = datetime.utcnow()

class_schema = StructType([
    StructField("class_id", StringType()),
    StructField("class_name", StringType()),
    StructField("parent_class_id", StringType()),
    StructField("parent_top_class", StringType()),
    StructField("class_path", StringType()),
    StructField("is_ai_inferrable", BooleanType()),
])

# COMMAND ----------

# PRODUCT DOMAIN (24 classes)
product_classes = [
    ("cls_product_type", "ProductType",       None,              "top_entity",       "/Entity/ProductType",                              False),
    ("cls_food",         "FoodProduct",        "cls_product_type","top_entity",       "/Entity/ProductType/FoodProduct",                  False),
    ("cls_fresh",        "FreshProduct",       "cls_food",        "top_entity",       "/Entity/ProductType/FoodProduct/FreshProduct",     False),
    ("cls_packaged",     "PackagedGood",       "cls_food",        "top_entity",       "/Entity/ProductType/FoodProduct/PackagedGood",     False),
    ("cls_nonfood",      "NonFoodProduct",     "cls_product_type","top_entity",       "/Entity/ProductType/NonFoodProduct",               False),
    # Dietary — THE KETO EXAMPLE LIVES HERE
    ("cls_dietary",      "DietaryAttribute",   None,              "top_attribute",    "/Attribute/DietaryAttribute",                      False),
    ("cls_keto",         "KetoCompliant",      "cls_dietary",     "top_attribute",    "/Attribute/DietaryAttribute/KetoCompliant",         True),
    ("cls_vegan",        "Vegan",              "cls_dietary",     "top_attribute",    "/Attribute/DietaryAttribute/Vegan",                 True),
    ("cls_plantbased",   "PlantBased",         "cls_vegan",       "top_attribute",    "/Attribute/DietaryAttribute/Vegan/PlantBased",      True),
    ("cls_gf",           "GlutenFree",         "cls_dietary",     "top_attribute",    "/Attribute/DietaryAttribute/GlutenFree",            True),
    ("cls_gf_cert",      "CertifiedGlutenFree","cls_gf",          "top_attribute",    "/Attribute/DietaryAttribute/GlutenFree/CertifiedGlutenFree", True),
    ("cls_organic",      "Organic",            "cls_dietary",     "top_attribute",    "/Attribute/DietaryAttribute/Organic",               True),
    ("cls_organic_usda", "USDACertifiedOrganic","cls_organic",    "top_attribute",    "/Attribute/DietaryAttribute/Organic/USDACertifiedOrganic", True),
    ("cls_dairyfree",    "DairyFree",          "cls_dietary",     "top_attribute",    "/Attribute/DietaryAttribute/DairyFree",             True),
    ("cls_paleo",        "Paleo",              "cls_dietary",     "top_attribute",    "/Attribute/DietaryAttribute/Paleo",                 True),
    ("cls_mediterranean","MediterraneanDiet",  "cls_dietary",     "top_attribute",    "/Attribute/DietaryAttribute/MediterraneanDiet",     True),
    # Allergens
    ("cls_allergen",     "AllergenFlag",       None,              "top_attribute",    "/Attribute/AllergenFlag",                          False),
    ("cls_nut_allergy",  "TreeNutAllergen",    "cls_allergen",    "top_attribute",    "/Attribute/AllergenFlag/TreeNutAllergen",           True),
    ("cls_dairy_allergy","DairyAllergen",      "cls_allergen",    "top_attribute",    "/Attribute/AllergenFlag/DairyAllergen",             True),
    ("cls_gluten_allergy","GlutenAllergen",    "cls_allergen",    "top_attribute",    "/Attribute/AllergenFlag/GlutenAllergen",            True),
    # Margin
    ("cls_margin",       "MarginSegment",      None,              "top_attribute",    "/Attribute/MarginSegment",                         False),
    ("cls_high_margin",  "HighMarginCategory", "cls_margin",      "top_attribute",    "/Attribute/MarginSegment/HighMarginCategory",       True),
    ("cls_loss_leader",  "LossLeader",         "cls_margin",      "top_attribute",    "/Attribute/MarginSegment/LossLeader",               True),
    ("cls_promo",        "PromotionalItem",    "cls_margin",      "top_attribute",    "/Attribute/MarginSegment/PromotionalItem",          True),
]

# COMMAND ----------

# CUSTOMER DOMAIN (16 classes — note: 15 listed + cls_fuel_user = 16 with cls_hh through cls_fuel_user)
customer_classes = [
    ("cls_hh",           "Household",          None,              "top_entity",       "/Entity/Household",                                False),
    ("cls_loyalty_hh",   "LoyaltyHousehold",   "cls_hh",          "top_entity",       "/Entity/Household/LoyaltyHousehold",               False),
    ("cls_anon_hh",      "AnonymousHousehold", "cls_hh",          "top_entity",       "/Entity/Household/AnonymousHousehold",              False),
    ("cls_lifecycle",    "LifecycleStage",     None,              "top_attribute",    "/Attribute/LifecycleStage",                        False),
    ("cls_new_cust",     "NewCustomer",        "cls_lifecycle",   "top_attribute",    "/Attribute/LifecycleStage/NewCustomer",             True),
    ("cls_active_cust",  "ActiveCustomer",     "cls_lifecycle",   "top_attribute",    "/Attribute/LifecycleStage/ActiveCustomer",          True),
    ("cls_atrisk_cust",  "AtRiskCustomer",     "cls_lifecycle",   "top_attribute",    "/Attribute/LifecycleStage/AtRiskCustomer",          True),
    ("cls_churned",      "ChurnedCustomer",    "cls_lifecycle",   "top_attribute",    "/Attribute/LifecycleStage/ChurnedCustomer",         True),
    ("cls_persona",      "CustomerPersona",    None,              "top_role",         "/Role/CustomerPersona",                            False),
    ("cls_hcm",          "HealthConsciousMom", "cls_persona",     "top_role",         "/Role/CustomerPersona/HealthConsciousMom",          True),
    ("cls_bff",          "BudgetFocusedFamily","cls_persona",     "top_role",         "/Role/CustomerPersona/BudgetFocusedFamily",         True),
    ("cls_keto_shopper", "KetoDieter",         "cls_persona",     "top_role",         "/Role/CustomerPersona/KetoDieter",                  True),
    ("cls_pharm_patient","PharmacyPatient",    "cls_persona",     "top_role",         "/Role/CustomerPersona/PharmacyPatient",              True),
    ("cls_fresh_first",  "FreshFirstShopper",  "cls_persona",     "top_role",         "/Role/CustomerPersona/FreshFirstShopper",            True),
    ("cls_fuel_user",    "FuelRewardsUser",    "cls_persona",     "top_role",         "/Role/CustomerPersona/FuelRewardsUser",              True),
]

# COMMAND ----------

# SUPPLY CHAIN DOMAIN (14 classes)
supply_chain_classes = [
    ("cls_node",         "Node",               None,              "top_location",     "/Location/Node",                                   False),
    ("cls_store_node",   "StoreNode",          "cls_node",        "top_location",     "/Location/Node/StoreNode",                         False),
    ("cls_dc_regional",  "DCNode",             "cls_node",        "top_location",     "/Location/Node/DCNode",                            False),
    ("cls_dc_fulfillment","FulfillmentDCNode", "cls_node",        "top_location",     "/Location/Node/FulfillmentDCNode",                 False),
    ("cls_3p_node",      "ThirdPartyNode",     "cls_node",        "top_location",     "/Location/Node/ThirdPartyNode",                    False),
    ("cls_fulfill",      "FulfillmentMethod",  None,              "top_relationship", "/Relationship/FulfillmentMethod",                  False),
    ("cls_sfs",          "ShipFromStore",      "cls_fulfill",     "top_relationship", "/Relationship/FulfillmentMethod/ShipFromStore",     False),
    ("cls_dc_fulfill",   "DCFulfillment",      "cls_fulfill",     "top_relationship", "/Relationship/FulfillmentMethod/DCFulfillment",     False),
    ("cls_instacart",    "InstacartDelivery",  "cls_fulfill",     "top_relationship", "/Relationship/FulfillmentMethod/InstacartDelivery", False),
    ("cls_ubereats",     "UberEatsDelivery",   "cls_fulfill",     "top_relationship", "/Relationship/FulfillmentMethod/UberEatsDelivery",  False),
    ("cls_inv_state",    "InventoryState",     None,              "top_attribute",    "/Attribute/InventoryState",                        False),
    ("cls_instock",      "InStock",            "cls_inv_state",   "top_attribute",    "/Attribute/InventoryState/InStock",                 True),
    ("cls_lowstock",     "LowStock",           "cls_inv_state",   "top_attribute",    "/Attribute/InventoryState/LowStock",                True),
    ("cls_oos",          "OutOfStock",         "cls_inv_state",   "top_attribute",    "/Attribute/InventoryState/OutOfStock",              True),
]

# COMMAND ----------

# STORE OPS DOMAIN (12 classes)
store_ops_classes = [
    ("cls_store_format",    "StoreFormat",      None,               "top_location",     "/Location/StoreFormat",                            False),
    ("cls_supermarket",     "Supermarket",      "cls_store_format", "top_location",     "/Location/StoreFormat/Supermarket",                 False),
    ("cls_marketplace",     "Marketplace",      "cls_store_format", "top_location",     "/Location/StoreFormat/Marketplace",                 False),
    ("cls_express",         "ExpressFormat",    "cls_store_format", "top_location",     "/Location/StoreFormat/ExpressFormat",               False),
    ("cls_store_event",     "StoreEvent",       None,               "top_event",        "/Event/StoreEvent",                                False),
    ("cls_grand_opening",   "GrandOpening",     "cls_store_event",  "top_event",        "/Event/StoreEvent/GrandOpening",                   False),
    ("cls_remodel",         "StoreRemodel",     "cls_store_event",  "top_event",        "/Event/StoreEvent/StoreRemodel",                   False),
    ("cls_seasonal_reset",  "SeasonalReset",    "cls_store_event",  "top_event",        "/Event/StoreEvent/SeasonalReset",                  False),
    ("cls_store_zone",      "StoreZone",        None,               "top_location",     "/Location/StoreZone",                              False),
    ("cls_front_end",       "FrontEnd",         "cls_store_zone",   "top_location",     "/Location/StoreZone/FrontEnd",                     False),
    ("cls_perimeter",       "Perimeter",        "cls_store_zone",   "top_location",     "/Location/StoreZone/Perimeter",                    False),
    ("cls_center_store",    "CenterStore",      "cls_store_zone",   "top_location",     "/Location/StoreZone/CenterStore",                  False),
]

# COMMAND ----------

# PHARMACY DOMAIN (11 classes)
pharmacy_classes = [
    ("cls_rx_product",   "RxProduct",          None,              "top_entity",       "/Entity/RxProduct",                                False),
    ("cls_generic_rx",   "GenericRx",          "cls_rx_product",  "top_entity",       "/Entity/RxProduct/GenericRx",                      False),
    ("cls_brand_rx",     "BrandNameRx",        "cls_rx_product",  "top_entity",       "/Entity/RxProduct/BrandNameRx",                    False),
    ("cls_therapeutic",  "TherapeuticClass",   None,              "top_attribute",    "/Attribute/TherapeuticClass",                      False),
    ("cls_cardio",       "Cardiovascular",     "cls_therapeutic", "top_attribute",    "/Attribute/TherapeuticClass/Cardiovascular",        True),
    ("cls_diabetes",     "Diabetes",           "cls_therapeutic", "top_attribute",    "/Attribute/TherapeuticClass/Diabetes",              True),
    ("cls_mental_health","MentalHealth",       "cls_therapeutic", "top_attribute",    "/Attribute/TherapeuticClass/MentalHealth",          True),
    ("cls_rx_event",     "RxEvent",            None,              "top_event",        "/Event/RxEvent",                                   False),
    ("cls_new_rx",       "NewRxFill",          "cls_rx_event",    "top_event",        "/Event/RxEvent/NewRxFill",                         False),
    ("cls_refill",       "RxRefill",           "cls_rx_event",    "top_event",        "/Event/RxEvent/RxRefill",                          False),
    ("cls_rx_abandon",   "RxAbandonment",      "cls_rx_event",    "top_event",        "/Event/RxEvent/RxAbandonment",                     False),
]

# COMMAND ----------

# FINANCE DOMAIN (6 classes)
finance_classes = [
    ("cls_rev_type",      "RevenueType",       None,              "top_quantity",     "/Quantity/RevenueType",                            False),
    ("cls_gross_rev",     "GrossRevenue",       "cls_rev_type",    "top_quantity",     "/Quantity/RevenueType/GrossRevenue",               False),
    ("cls_net_rev",       "NetRevenue",         "cls_rev_type",    "top_quantity",     "/Quantity/RevenueType/NetRevenue",                 False),
    ("cls_media_rev",     "MediaRevenue",       "cls_rev_type",    "top_quantity",     "/Quantity/RevenueType/MediaRevenue",               False),
    ("cls_fulfill_cost",  "FulfillmentCost",    None,              "top_quantity",     "/Quantity/FulfillmentCost",                        False),
    ("cls_lastmile_cost", "LastMileCost",       None,              "top_quantity",     "/Quantity/LastMileCost",                           False),
]

# COMMAND ----------

# MEDIA / KPM DOMAIN (8 classes)
media_classes = [
    ("cls_audience",     "AudienceSegment",       None,              "top_role",      "/Role/AudienceSegment",                            False),
    ("cls_cpg_audience", "CPGDefinedAudience",    "cls_audience",    "top_role",      "/Role/AudienceSegment/CPGDefinedAudience",          False),
    ("cls_kr_audience",  "RetailDefinedAudience", "cls_audience",    "top_role",      "/Role/AudienceSegment/RetailDefinedAudience",       False),
    ("cls_ad_campaign",  "AdCampaign",            None,              "top_event",     "/Event/AdCampaign",                                False),
    ("cls_display_ad",   "DisplayAdCampaign",     "cls_ad_campaign", "top_event",     "/Event/AdCampaign/DisplayAdCampaign",               False),
    ("cls_attribution",  "AttributionEvent",      None,              "top_event",     "/Event/AttributionEvent",                          False),
    ("cls_impression",   "AdImpression",          "cls_attribution", "top_event",     "/Event/AttributionEvent/AdImpression",              False),
    ("cls_conversion",   "PostExposurePurchase",  "cls_attribution", "top_event",     "/Event/AttributionEvent/PostExposurePurchase",      False),
]

# COMMAND ----------

# Write all domains to v2_ontology.tier2_*.classes
domain_table_map = {
    "v2_ontology.tier2_product.classes":       product_classes,
    "v2_ontology.tier2_customer.classes":       customer_classes,
    "v2_ontology.tier2_supply_chain.classes":   supply_chain_classes,
    "v2_ontology.tier2_store_ops.classes":      store_ops_classes,
    "v2_ontology.tier2_pharmacy.classes":       pharmacy_classes,
    "v2_ontology.tier2_finance.classes":        finance_classes,
    "v2_ontology.tier2_media.classes":          media_classes,
}

total_classes = 0
for table, classes in domain_table_map.items():
    rows = [Row(
        class_id=c[0], class_name=c[1], parent_class_id=c[2],
        parent_top_class=c[3], class_path=c[4], is_ai_inferrable=c[5]
    ) for c in classes]

    domain_name = table.split(".")[1]

    spark.createDataFrame(rows, class_schema) \
        .withColumn("domain", F.lit(domain_name)) \
        .withColumn("ontology_version", F.lit("1.0.0")) \
        .withColumn("is_active", F.lit(True)) \
        .withColumn("description", F.concat(F.lit("Ontology class: "), F.col("class_name"))) \
        .withColumn("created_by", F.lit("ontology_bootstrap")) \
        .withColumn("created_ts", F.lit(now)) \
        .withColumn("updated_ts", F.lit(now)) \
        .write.mode("overwrite").saveAsTable(table)

    count = len(classes)
    total_classes += count
    print(f"  {count} classes -> {table}")

# COMMAND ----------

# Verification: print counts per domain
print("\n--- Domain Class Counts ---")
for table in domain_table_map.keys():
    cnt = spark.table(table).count()
    print(f"  {table}: {cnt} classes")

print(f"\nTotal domain classes seeded: {total_classes}")
print("All 7 domains seeded successfully!")
