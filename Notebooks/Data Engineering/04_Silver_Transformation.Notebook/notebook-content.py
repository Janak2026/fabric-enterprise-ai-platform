# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   },
# META   "dependencies": {
# META     "lakehouse": {
# META       "default_lakehouse": "170d9a5a-7e61-4d0f-8908-f16ec4daf847",
# META       "default_lakehouse_name": "AI_ML_LakeHouse",
# META       "default_lakehouse_workspace_id": "d7ae502d-247b-4ea5-857f-127fff869a69",
# META       "known_lakehouses": [
# META         {
# META           "id": "170d9a5a-7e61-4d0f-8908-f16ec4daf847"
# META         }
# META       ]
# META     }
# META   }
# META }

# MARKDOWN ********************

# # 04 - Silver Transformation
# 
# ## Objective
# 
# Transform Bronze Delta tables into standardized, validated, business-ready Silver tables.
# 
# This notebook performs:
# 
# - Load Bronze Delta tables
# - Data quality validation
# - Data type standardization
# - Business transformations
# - Relationship validation
# - Create Silver Delta tables
# - Validation summary
# 
# ### Layer
# 
# Bronze ➜ Silver

# CELL ********************

# cell 1:
# ==========================================================
# Import Required Libraries
# ==========================================================

# Spark Session
from pyspark.sql import SparkSession

# PySpark SQL Functions
from pyspark.sql import functions as F

# PySpark Data Types
from pyspark.sql.types import (
    StringType,
    IntegerType,
    DoubleType,
    FloatType,
    LongType,
    DateType,
    TimestampType,
    BooleanType
)

# Window Functions
from pyspark.sql.window import Window

# Python Libraries
from datetime import datetime

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Cell 2:
# ==========================================================
# Project Configuration
# ==========================================================

from datetime import datetime

spark = SparkSession.builder.getOrCreate()

PROJECT_NAME = "Fabric Enterprise AI Platform"
NOTEBOOK_NAME = "04_Silver_Transformation"

SOURCE_LAYER = "Bronze"
TARGET_LAYER = "Silver"

print("=" * 70)
print(PROJECT_NAME)
print("=" * 70)

print(f"Notebook         : {NOTEBOOK_NAME}")
print(f"Execution Time   : {datetime.now():%Y-%m-%d %H:%M:%S}")
print(f"Source Layer     : {SOURCE_LAYER}")
print(f"Target Layer     : {TARGET_LAYER}")

print("=" * 70)
print("Configuration Loaded Successfully")
print("=" * 70)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Cell 3:
# ============================================================
# Bronze Tables
# ============================================================

bronze_tables = [
    "bronze_customers",
    "bronze_orders",
    "bronze_order_items",
    "bronze_order_payments",
    "bronze_order_reviews",
    "bronze_products",
    "bronze_sellers",
    "bronze_geolocation"
]

bronze_dataframes = {}

print("=" * 60)
print("Loading Bronze Tables...")
print("=" * 60)

for table in bronze_tables:
    
    df = spark.table(table)
    
    bronze_dataframes[table] = df
    
    print(f"{table:<30} Rows : {df.count():>10,}")

print("=" * 60)
print("Bronze tables loaded successfully.")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Cell 4:
# ============================================================
# Bronze Data Quality Assessment
# ============================================================

print("=" * 90)
print("BRONZE DATA QUALITY ASSESSMENT")
print("=" * 90)

for table_name, df in bronze_dataframes.items():

    print(f"\n{'-' * 90}")
    print(f"Table : {table_name}")
    print(f"{'-' * 90}")

    # Row & Column Counts
    total_rows = df.count()
    total_columns = len(df.columns)

    print(f"Rows          : {total_rows:,}")
    print(f"Columns       : {total_columns}")

    # Duplicate Rows
    duplicate_rows = total_rows - df.dropDuplicates().count()

    print(f"Duplicate Rows: {duplicate_rows:,}")

    # Null Count Per Column
    print("\nNull Analysis")

    null_df = df.select([
        F.count(F.when(F.col(c).isNull(), c)).alias(c)
        for c in df.columns
    ])

    null_df.show(truncate=False)

print("=" * 90)
print("Bronze Data Quality Assessment Completed Successfully.")
print("=" * 90)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Cell 5:
# ============================================================
# Create Working DataFrames
# ============================================================

print("=" * 60)
print("Creating Working DataFrames...")
print("=" * 60)

customers_df      = bronze_dataframes["bronze_customers"]
orders_df         = bronze_dataframes["bronze_orders"]
order_items_df    = bronze_dataframes["bronze_order_items"]
payments_df       = bronze_dataframes["bronze_order_payments"]
reviews_df        = bronze_dataframes["bronze_order_reviews"]
products_df       = bronze_dataframes["bronze_products"]
sellers_df        = bronze_dataframes["bronze_sellers"]
geolocation_df    = bronze_dataframes["bronze_geolocation"]

print("✓ customers_df")
print("✓ orders_df")
print("✓ order_items_df")
print("✓ payments_df")
print("✓ reviews_df")
print("✓ products_df")
print("✓ sellers_df")
print("✓ geolocation_df")

print("=" * 60)
print("Working DataFrames Created Successfully")
print("=" * 60)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# # Create Working DataFrames
# 
# ## Objective
# 
# This section creates working DataFrames from the Bronze layer.
# 
# The Bronze tables remain unchanged throughout the notebook.
# All business transformations are applied only to the working DataFrames,
# ensuring clear data lineage and preserving the integrity of the Bronze layer.
# 
# Architecture:
# 
# Bronze Tables
#       │
#       ▼
# Working DataFrames
#       │
#       ▼
# Business Transformations
#       │
#       ▼
# Silver Tables

# CELL ********************

# Cell 6:
# ============================================================
# Create Working DataFrames
# ============================================================

print("=" * 90)
print("CREATING WORKING DATAFRAMES")
print("=" * 90)

customers_df = bronze_dataframes["bronze_customers"]
orders_df = bronze_dataframes["bronze_orders"]
order_items_df = bronze_dataframes["bronze_order_items"]
payments_df = bronze_dataframes["bronze_order_payments"]
reviews_df = bronze_dataframes["bronze_order_reviews"]
products_df = bronze_dataframes["bronze_products"]
sellers_df = bronze_dataframes["bronze_sellers"]
geolocation_df = bronze_dataframes["bronze_geolocation"]

print("✓ customers_df")
print("✓ orders_df")
print("✓ order_items_df")
print("✓ payments_df")
print("✓ reviews_df")
print("✓ products_df")
print("✓ sellers_df")
print("✓ geolocation_df")

print("=" * 90)
print("WORKING DATAFRAMES CREATED SUCCESSFULLY")
print("=" * 90)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# # Customer Transformation (Bronze → Silver)
# 
# ## Objective
# 
# This section transforms the Bronze Customer dataset into a clean, standardized Silver dataset.
# 
# ### Business Rules
# 
# - Remove duplicate customers
# - Trim leading/trailing spaces
# - Convert empty strings to NULL
# - Standardize city names (Proper Case)
# - Standardize state codes (Upper Case)
# - Add Silver audit columns
# 
# Output Table:
# 
# silver_customers

# CELL ********************

# Cell 7:
# ==========================================================
# Customer Transformation
# Bronze -> Silver
# ==========================================================

print("=" * 90)
print("CUSTOMER TRANSFORMATION")
print("=" * 90)

# ----------------------------------------------------------
# Remove Duplicate Customers
# ----------------------------------------------------------

silver_customers = customers_df.dropDuplicates(["customer_id"])

print(f"Rows after duplicate removal : {silver_customers.count():,}")

# ----------------------------------------------------------
# Identify String Columns
# ----------------------------------------------------------

string_columns = [
    field.name
    for field in silver_customers.schema.fields
    if isinstance(field.dataType, StringType)
]

# ----------------------------------------------------------
# Trim Leading & Trailing Spaces
# ----------------------------------------------------------

for column in string_columns:
    silver_customers = silver_customers.withColumn(
        column,
        F.trim(F.col(column))
    )

# ----------------------------------------------------------
# Convert Empty Strings to NULL
# ----------------------------------------------------------

for column in string_columns:
    silver_customers = silver_customers.withColumn(
        column,
        F.when(F.col(column) == "", None)
         .otherwise(F.col(column))
    )

# ----------------------------------------------------------
# Standardize Customer City
# ----------------------------------------------------------

silver_customers = silver_customers.withColumn(
    "customer_city",
    F.initcap(F.col("customer_city"))
)

# ----------------------------------------------------------
# Standardize Customer State
# ----------------------------------------------------------

silver_customers = silver_customers.withColumn(
    "customer_state",
    F.upper(F.col("customer_state"))
)

# ----------------------------------------------------------
# Add Silver Audit Columns
# ----------------------------------------------------------

silver_customers = (
    silver_customers

    .withColumn(
        "silver_created_timestamp",
        F.current_timestamp()
    )

    .withColumn(
        "source_layer",
        F.lit("Bronze")
    )

    .withColumn(
        "target_layer",
        F.lit("Silver")
    )

    .withColumn(
        "notebook_name",
        F.lit(NOTEBOOK_NAME)
    )
)

print("✓ Customer transformation completed successfully.")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Cell 8:
# ==========================================================
# Customer Validation
# ==========================================================

print("=" * 90)
print("CUSTOMER VALIDATION")
print("=" * 90)

print(f"Total Rows    : {silver_customers.count():,}")
print(f"Total Columns : {len(silver_customers.columns)}")

print("\nSchema")
silver_customers.printSchema()

print("\nSample Records")
silver_customers.show(5, truncate=False)

print("=" * 90)
print("CUSTOMER VALIDATION COMPLETED")
print("=" * 90)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# # Orders Transformation (Bronze → Silver)
# 
# ## Objective
# 
# Transform the Bronze Orders dataset into a standardized Silver dataset by applying
# business rules, data type conversions, and derived business attributes.
# 
# ### Business Rules
# 
# - Remove duplicate orders
# - Convert timestamps to TimestampType
# - Derive calendar attributes
# - Calculate delivery metrics
# - Create business delivery status
# - Add Silver audit columns
# 
# Output Table
# 
# silver_orders

# CELL ********************

# Cell 9:
# ==========================================================
# Orders Transformation
# Bronze -> Silver
# ==========================================================

print("=" * 90)
print("ORDERS TRANSFORMATION")
print("=" * 90)

# ----------------------------------------------------------
# Remove Duplicate Orders
# ----------------------------------------------------------

silver_orders = orders_df.dropDuplicates(["order_id"])

print(f"Rows after duplicate removal : {silver_orders.count():,}")

# ----------------------------------------------------------
# Convert Timestamp Columns
# ----------------------------------------------------------

timestamp_columns = [
    "order_purchase_timestamp",
    "order_approved_at",
    "order_delivered_carrier_date",
    "order_delivered_customer_date",
    "order_estimated_delivery_date"
]

for column in timestamp_columns:

    silver_orders = silver_orders.withColumn(
        column,
        F.to_timestamp(F.col(column))
    )

# ----------------------------------------------------------
# Create Calendar Attributes
# ----------------------------------------------------------

silver_orders = (

    silver_orders

    .withColumn(
        "order_year",
        F.year("order_purchase_timestamp")
    )

    .withColumn(
        "order_month",
        F.month("order_purchase_timestamp")
    )

    .withColumn(
        "order_quarter",
        F.quarter("order_purchase_timestamp")
    )

    .withColumn(
        "order_week",
        F.weekofyear("order_purchase_timestamp")
    )

    .withColumn(
        "order_day",
        F.dayofmonth("order_purchase_timestamp")
    )

    .withColumn(
        "order_day_name",
        F.date_format(
            "order_purchase_timestamp",
            "EEEE"
        )
    )

)

# ----------------------------------------------------------
# Delivery Duration
# ----------------------------------------------------------

silver_orders = silver_orders.withColumn(

    "delivery_days",

    F.datediff(

        F.col("order_delivered_customer_date"),

        F.col("order_purchase_timestamp")

    )

)

# ----------------------------------------------------------
# Delivery Delay
# ----------------------------------------------------------

silver_orders = silver_orders.withColumn(

    "delivery_delay_days",

    F.datediff(

        F.col("order_delivered_customer_date"),

        F.col("order_estimated_delivery_date")

    )

)

# ----------------------------------------------------------
# Delivery Status
# ----------------------------------------------------------

silver_orders = silver_orders.withColumn(

    "delivery_status",

    F.when(

        F.col("order_status") == "delivered",

        "Delivered"

    )

    .when(

        F.col("order_status") == "cancelled",

        "Cancelled"

    )

    .when(

        F.col("order_status") == "shipped",

        "In Transit"

    )

    .otherwise(

        "Processing"

    )

)

# ----------------------------------------------------------
# Early / Late Delivery Indicator
# ----------------------------------------------------------

silver_orders = silver_orders.withColumn(

    "delivery_performance",

    F.when(

        F.col("delivery_delay_days") < 0,

        "Early"

    )

    .when(

        F.col("delivery_delay_days") == 0,

        "On Time"

    )

    .when(

        F.col("delivery_delay_days") > 0,

        "Late"

    )

    .otherwise(

        "Unknown"

    )

)

# ----------------------------------------------------------
# Audit Columns
# ----------------------------------------------------------

silver_orders = (

    silver_orders

    .withColumn(
        "silver_created_timestamp",
        F.current_timestamp()
    )

    .withColumn(
        "source_layer",
        F.lit("Bronze")
    )

    .withColumn(
        "target_layer",
        F.lit("Silver")
    )

    .withColumn(
        "notebook_name",
        F.lit(NOTEBOOK_NAME)
    )

)

print("✓ Orders transformation completed successfully.")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Cell 10:
# ==========================================================
# Orders Validation
# ==========================================================

print("=" * 90)
print("ORDERS VALIDATION")
print("=" * 90)

print(f"Rows    : {silver_orders.count():,}")
print(f"Columns : {len(silver_orders.columns)}")

print("\nSchema")
silver_orders.printSchema()

print("\nDelivery Status Distribution")

silver_orders.groupBy(
    "delivery_status"
).count().show()

print("\nDelivery Performance Distribution")

silver_orders.groupBy(
    "delivery_performance"
).count().show()

print("\nSample Records")

silver_orders.show(
    5,
    truncate=False
)

print("=" * 90)
print("ORDERS VALIDATION COMPLETED")
print("=" * 90)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# # Order Items Transformation (Bronze → Silver)
# 
# ## Objective
# 
# Transform the Bronze Order Items dataset into a clean Silver dataset by applying
# data quality rules, standardization, and audit enrichment.
# 
# ### Business Rules
# 
# - Remove duplicate records
# - Convert monetary columns to Double
# - Round monetary values to 2 decimal places
# - Validate positive values
# - Add Silver audit columns
# 
# Output Table
# 
# silver_order_items

# CELL ********************

# Cell 11:
# ==========================================================
# Order Items Transformation
# Bronze -> Silver
# ==========================================================

print("=" * 90)
print("ORDER ITEMS TRANSFORMATION")
print("=" * 90)

# ----------------------------------------------------------
# Remove Duplicate Records
# ----------------------------------------------------------

silver_order_items = order_items_df.dropDuplicates()

print(f"Rows after duplicate removal : {silver_order_items.count():,}")

# ----------------------------------------------------------
# Convert Numeric Columns
# ----------------------------------------------------------

numeric_columns = [
    "price",
    "freight_value"
]

for column in numeric_columns:

    silver_order_items = silver_order_items.withColumn(
        column,
        F.col(column).cast("double")
    )

# ----------------------------------------------------------
# Round Monetary Values
# ----------------------------------------------------------

for column in numeric_columns:

    silver_order_items = silver_order_items.withColumn(
        column,
        F.round(F.col(column), 2)
    )

# ----------------------------------------------------------
# Replace Negative Values with NULL
# ----------------------------------------------------------

silver_order_items = (

    silver_order_items

    .withColumn(
        "price",
        F.when(F.col("price") >= 0, F.col("price"))
         .otherwise(None)
    )

    .withColumn(
        "freight_value",
        F.when(F.col("freight_value") >= 0, F.col("freight_value"))
         .otherwise(None)
    )

)

# ----------------------------------------------------------
# Derived Business Columns
# ----------------------------------------------------------

silver_order_items = (

    silver_order_items

    .withColumn(
        "total_item_value",
        F.round(
            F.col("price") + F.col("freight_value"),
            2
        )
    )

    .withColumn(
        "is_free_shipping",
        F.when(
            F.col("freight_value") == 0,
            True
        ).otherwise(False)
    )

)

# ----------------------------------------------------------
# Add Silver Audit Columns
# ----------------------------------------------------------

silver_order_items = (

    silver_order_items

    .withColumn(
        "silver_created_timestamp",
        F.current_timestamp()
    )

    .withColumn(
        "source_layer",
        F.lit("Bronze")
    )

    .withColumn(
        "target_layer",
        F.lit("Silver")
    )

    .withColumn(
        "notebook_name",
        F.lit(NOTEBOOK_NAME)
    )

)

print("✓ Order Items transformation completed successfully.")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Cell 12:
# ==========================================================
# Order Items Validation
# ==========================================================

print("=" * 90)
print("ORDER ITEMS VALIDATION")
print("=" * 90)

print(f"Rows    : {silver_order_items.count():,}")
print(f"Columns : {len(silver_order_items.columns)}")

print("\nSchema")
silver_order_items.printSchema()

print("\nSummary Statistics")

silver_order_items.select(

    F.round(F.avg("price"), 2).alias("Average Price"),
    F.round(F.avg("freight_value"), 2).alias("Average Freight"),
    F.round(F.avg("total_item_value"), 2).alias("Average Total")

).show()

print("\nFree Shipping Distribution")

silver_order_items.groupBy(
    "is_free_shipping"
).count().show()

print("\nSample Records")

silver_order_items.show(
    5,
    truncate=False
)

print("=" * 90)
print("ORDER ITEMS VALIDATION COMPLETED")
print("=" * 90)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# # Products Transformation (Bronze → Silver)
# 
# ## Objective
# 
# Transform the Bronze Products dataset into a standardized Silver Products table.
# 
# ### Business Rules
# 
# - Remove duplicate products
# - Standardize string columns
# - Convert empty strings to NULL
# - Handle missing numeric values
# - Add Silver audit columns
# 
# Output Table
# 
# silver_products

# CELL ********************

# Cell 13:
# ==========================================================
# Products Transformation
# Bronze -> Silver
# ==========================================================

print("=" * 90)
print("PRODUCTS TRANSFORMATION")
print("=" * 90)

# ----------------------------------------------------------
# Remove Duplicate Products
# ----------------------------------------------------------

silver_products = products_df.dropDuplicates(["product_id"])

print(f"Rows after duplicate removal : {silver_products.count():,}")

# ----------------------------------------------------------
# Standardize Source Column Names
# (Correct Olist source typo: lenght -> length)
# ----------------------------------------------------------

silver_products = (

    silver_products

    .withColumnRenamed(
        "product_name_lenght",
        "product_name_length"
    )

    .withColumnRenamed(
        "product_description_lenght",
        "product_description_length"
    )

)

# ----------------------------------------------------------
# Identify String Columns
# ----------------------------------------------------------

string_columns = [

    field.name

    for field in silver_products.schema.fields

    if isinstance(field.dataType, StringType)

]

# ----------------------------------------------------------
# Trim String Columns
# ----------------------------------------------------------

for column in string_columns:

    silver_products = silver_products.withColumn(

        column,

        F.trim(F.col(column))

    )

# ----------------------------------------------------------
# Convert Empty Strings to NULL
# ----------------------------------------------------------

for column in string_columns:

    silver_products = silver_products.withColumn(

        column,

        F.when(

            F.col(column) == "",

            F.lit(None)

        ).otherwise(

            F.col(column)

        )

    )

# ----------------------------------------------------------
# Standardize Product Category
# ----------------------------------------------------------

silver_products = silver_products.withColumn(

    "product_category_name",

    F.initcap(F.col("product_category_name"))

)

# ----------------------------------------------------------
# Cast Numeric Columns
# ----------------------------------------------------------

numeric_columns = {

    "product_name_length": "int",
    "product_description_length": "int",
    "product_photos_qty": "int",
    "product_weight_g": "double",
    "product_length_cm": "double",
    "product_height_cm": "double",
    "product_width_cm": "double"

}

for column, datatype in numeric_columns.items():

    silver_products = silver_products.withColumn(

        column,

        F.col(column).cast(datatype)

    )

# ----------------------------------------------------------
# Replace Negative Values with NULL
# ----------------------------------------------------------

for column in numeric_columns.keys():

    silver_products = silver_products.withColumn(

        column,

        F.when(

            F.col(column).isNull(),

            F.lit(None)

        )

        .when(

            F.col(column) >= 0,

            F.col(column)

        )

        .otherwise(

            F.lit(None)

        )

    )

# ----------------------------------------------------------
# Add Silver Audit Columns
# ----------------------------------------------------------

silver_products = (

    silver_products

    .withColumn(

        "silver_created_timestamp",

        F.current_timestamp()

    )

    .withColumn(

        "source_layer",

        F.lit("Bronze")

    )

    .withColumn(

        "target_layer",

        F.lit("Silver")

    )

    .withColumn(

        "notebook_name",

        F.lit(NOTEBOOK_NAME)

    )

)

print("✓ Products transformation completed successfully.")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Cell 14:
# ==========================================================
# Products Validation
# ==========================================================

print("=" * 90)
print("PRODUCTS VALIDATION")
print("=" * 90)

print(f"Rows    : {silver_products.count():,}")
print(f"Columns : {len(silver_products.columns)}")

print("\nSchema")
silver_products.printSchema()

print("\nTop Product Categories")

silver_products.groupBy(
    "product_category_name"
).count().orderBy(
    F.desc("count")
).show(10, truncate=False)

print("\nMissing Values")

silver_products.select(

    *[
        F.sum(F.col(c).isNull().cast("int")).alias(c)
        for c in silver_products.columns
    ]

).show(truncate=False)

print("\nSample Records")

silver_products.show(
    5,
    truncate=False
)

print("=" * 90)
print("PRODUCTS VALIDATION COMPLETED")
print("=" * 90)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Cell 15:
# ==========================================================
# Sellers Transformation
# Bronze -> Silver
# ==========================================================

print("=" * 90)
print("SELLERS TRANSFORMATION")
print("=" * 90)

# ----------------------------------------------------------
# Remove Duplicate Sellers
# ----------------------------------------------------------

silver_sellers = sellers_df.dropDuplicates(["seller_id"])

print(f"Rows after duplicate removal : {silver_sellers.count():,}")

# ----------------------------------------------------------
# Identify String Columns
# ----------------------------------------------------------

string_columns = [

    field.name

    for field in silver_sellers.schema.fields

    if isinstance(field.dataType, StringType)

]

# ----------------------------------------------------------
# Trim String Columns
# ----------------------------------------------------------

for column in string_columns:

    silver_sellers = silver_sellers.withColumn(

        column,

        F.trim(F.col(column))

    )

# ----------------------------------------------------------
# Convert Empty Strings to NULL
# ----------------------------------------------------------

for column in string_columns:

    silver_sellers = silver_sellers.withColumn(

        column,

        F.when(

            F.col(column) == "",

            F.lit(None)

        ).otherwise(

            F.col(column)

        )

    )

# ----------------------------------------------------------
# Standardize City and State
# ----------------------------------------------------------

silver_sellers = (

    silver_sellers

    .withColumn(

        "seller_city",

        F.initcap(F.col("seller_city"))

    )

    .withColumn(

        "seller_state",

        F.upper(F.col("seller_state"))

    )

)

# ----------------------------------------------------------
# Cast ZIP Code Prefix
# ----------------------------------------------------------

silver_sellers = silver_sellers.withColumn(

    "seller_zip_code_prefix",

    F.col("seller_zip_code_prefix").cast("integer")

)

# ----------------------------------------------------------
# Replace Negative ZIP Codes with NULL
# ----------------------------------------------------------

silver_sellers = silver_sellers.withColumn(

    "seller_zip_code_prefix",

    F.when(

        F.col("seller_zip_code_prefix").isNull(),

        F.lit(None)

    )

    .when(

        F.col("seller_zip_code_prefix") >= 0,

        F.col("seller_zip_code_prefix")

    )

    .otherwise(

        F.lit(None)

    )

)

# ----------------------------------------------------------
# Add Silver Audit Columns
# ----------------------------------------------------------

silver_sellers = (

    silver_sellers

    .withColumn(

        "silver_created_timestamp",

        F.current_timestamp()

    )

    .withColumn(

        "source_layer",

        F.lit("Bronze")

    )

    .withColumn(

        "target_layer",

        F.lit("Silver")

    )

    .withColumn(

        "notebook_name",

        F.lit(NOTEBOOK_NAME)

    )

)

print("✓ Sellers transformation completed successfully.")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Cell 16:
# ==========================================================
# Sellers Validation
# ==========================================================

print("=" * 90)
print("SELLERS VALIDATION")
print("=" * 90)

print(f"Rows    : {silver_sellers.count():,}")
print(f"Columns : {len(silver_sellers.columns)}")

print("\nSchema")
silver_sellers.printSchema()

print("\nTop Seller States")

silver_sellers.groupBy(

    "seller_state"

).count().orderBy(

    F.desc("count")

).show(10, truncate=False)

print("\nMissing Values")

silver_sellers.select(

    *[
        F.sum(
            F.col(c).isNull().cast("int")
        ).alias(c)

        for c in silver_sellers.columns
    ]

).show(truncate=False)

print("\nSample Records")

silver_sellers.show(

    5,

    truncate=False

)

print("=" * 90)
print("SELLERS VALIDATION COMPLETED")
print("=" * 90)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Cell 17:
# ==========================================================
# Order Payments Transformation
# Bronze -> Silver
# ==========================================================

print("=" * 90)
print("ORDER PAYMENTS TRANSFORMATION")
print("=" * 90)

# ----------------------------------------------------------
# Remove Duplicate Payments
# ----------------------------------------------------------

silver_payments = payments_df.dropDuplicates(
    ["order_id", "payment_sequential"]
)

print(f"Rows after duplicate removal : {silver_payments.count():,}")

# ----------------------------------------------------------
# Identify String Columns
# ----------------------------------------------------------

string_columns = [

    field.name

    for field in silver_payments.schema.fields

    if isinstance(field.dataType, StringType)

]

# ----------------------------------------------------------
# Trim String Columns
# ----------------------------------------------------------

for column in string_columns:

    silver_payments = silver_payments.withColumn(

        column,

        F.trim(F.col(column))

    )

# ----------------------------------------------------------
# Convert Empty Strings to NULL
# ----------------------------------------------------------

for column in string_columns:

    silver_payments = silver_payments.withColumn(

        column,

        F.when(
            F.col(column) == "",
            F.lit(None)
        ).otherwise(
            F.col(column)
        )

    )

# ----------------------------------------------------------
# Standardize Payment Type
# ----------------------------------------------------------

silver_payments = silver_payments.withColumn(

    "payment_type",

    F.initcap(F.col("payment_type"))

)

# ----------------------------------------------------------
# Cast Numeric Columns
# ----------------------------------------------------------

numeric_columns = {

    "payment_sequential": "int",
    "payment_installments": "int",
    "payment_value": "double"

}

for column, datatype in numeric_columns.items():

    silver_payments = silver_payments.withColumn(

        column,

        F.col(column).cast(datatype)

    )

# ----------------------------------------------------------
# Replace Negative Values with NULL
# ----------------------------------------------------------

for column in numeric_columns.keys():

    silver_payments = silver_payments.withColumn(

        column,

        F.when(
            F.col(column).isNull(),
            F.lit(None)
        )

        .when(
            F.col(column) >= 0,
            F.col(column)
        )

        .otherwise(
            F.lit(None)
        )

    )

# ----------------------------------------------------------
# Add Silver Audit Columns
# ----------------------------------------------------------

silver_payments = (

    silver_payments

    .withColumn(
        "silver_created_timestamp",
        F.current_timestamp()
    )

    .withColumn(
        "source_layer",
        F.lit("Bronze")
    )

    .withColumn(
        "target_layer",
        F.lit("Silver")
    )

    .withColumn(
        "notebook_name",
        F.lit(NOTEBOOK_NAME)
    )

)

print("✓ Order Payments transformation completed successfully.")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Cell 18:
# ==========================================================
# Order Payments Validation
# ==========================================================

print("=" * 90)
print("ORDER PAYMENTS VALIDATION")
print("=" * 90)

print(f"Rows    : {silver_payments.count():,}")
print(f"Columns : {len(silver_payments.columns)}")

print("\nSchema")

silver_payments.printSchema()

# ----------------------------------------------------------
# Payment Type Distribution
# ----------------------------------------------------------

print("\nPayment Type Distribution")

silver_payments.groupBy(

    "payment_type"

).count().orderBy(

    F.desc("count")

).show(

    truncate=False

)

# ----------------------------------------------------------
# Missing Values
# ----------------------------------------------------------

print("\nMissing Values")

silver_payments.select(

    *[

        F.sum(

            F.col(c).isNull().cast("int")

        ).alias(c)

        for c in silver_payments.columns

    ]

).show(truncate=False)

# ----------------------------------------------------------
# Payment Statistics
# ----------------------------------------------------------

print("\nPayment Value Statistics")

silver_payments.select(

    "payment_value"

).describe().show()

# ----------------------------------------------------------
# Sample Records
# ----------------------------------------------------------

print("\nSample Records")

silver_payments.show(

    5,

    truncate=False

)

print("=" * 90)
print("ORDER PAYMENTS VALIDATION COMPLETED")
print("=" * 90)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Cell 19
# ==========================================================
# Order Reviews Transformation
# Bronze -> Silver
# ==========================================================

print("=" * 90)
print("ORDER REVIEWS TRANSFORMATION")
print("=" * 90)

# ----------------------------------------------------------
# Remove Duplicate Reviews
# ----------------------------------------------------------

silver_reviews = reviews_df.dropDuplicates(["review_id"])

print(f"Rows after duplicate removal : {silver_reviews.count():,}")

# ----------------------------------------------------------
# Identify String Columns
# ----------------------------------------------------------

string_columns = [

    field.name

    for field in silver_reviews.schema.fields

    if isinstance(field.dataType, StringType)

]

# ----------------------------------------------------------
# Trim String Columns
# ----------------------------------------------------------

for column in string_columns:

    silver_reviews = silver_reviews.withColumn(

        column,

        F.trim(F.col(column))

    )

# ----------------------------------------------------------
# Convert Empty Strings to NULL
# ----------------------------------------------------------

for column in string_columns:

    silver_reviews = silver_reviews.withColumn(

        column,

        F.when(
            F.col(column) == "",
            F.lit(None)
        ).otherwise(
            F.col(column)
        )

    )

# ----------------------------------------------------------
# Cast Review Score
# ----------------------------------------------------------

silver_reviews = silver_reviews.withColumn(

    "review_score",

    F.col("review_score").cast("integer")

)

# ----------------------------------------------------------
# Validate Review Score
# Keep only values between 1 and 5
# ----------------------------------------------------------

silver_reviews = silver_reviews.withColumn(

    "review_score",

    F.when(

        F.col("review_score").between(1, 5),

        F.col("review_score")

    ).otherwise(

        F.lit(None)

    )

)

# ----------------------------------------------------------
# Cast Date Columns
# ----------------------------------------------------------

date_columns = [

    "review_creation_date",
    "review_answer_timestamp"

]

for column in date_columns:

    silver_reviews = silver_reviews.withColumn(

        column,

        F.to_timestamp(F.col(column))

    )

# ----------------------------------------------------------
# Add Silver Audit Columns
# ----------------------------------------------------------

silver_reviews = (

    silver_reviews

    .withColumn(

        "silver_created_timestamp",

        F.current_timestamp()

    )

    .withColumn(

        "source_layer",

        F.lit("Bronze")

    )

    .withColumn(

        "target_layer",

        F.lit("Silver")

    )

    .withColumn(

        "notebook_name",

        F.lit(NOTEBOOK_NAME)

    )

)

print("✓ Order Reviews transformation completed successfully.")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Cell 20:
# ==========================================================
# Order Reviews Validation
# ==========================================================

print("=" * 90)
print("ORDER REVIEWS VALIDATION")
print("=" * 90)

print(f"Rows    : {silver_reviews.count():,}")
print(f"Columns : {len(silver_reviews.columns)}")

# ----------------------------------------------------------
# Schema
# ----------------------------------------------------------

print("\nSchema")

silver_reviews.printSchema()

# ----------------------------------------------------------
# Review Score Distribution
# ----------------------------------------------------------

print("\nReview Score Distribution")

silver_reviews.groupBy(

    "review_score"

).count().orderBy(

    "review_score"

).show()

# ----------------------------------------------------------
# Missing Values
# ----------------------------------------------------------

print("\nMissing Values")

silver_reviews.select(

    *[

        F.sum(

            F.col(c).isNull().cast("int")

        ).alias(c)

        for c in silver_reviews.columns

    ]

).show(truncate=False)

# ----------------------------------------------------------
# Review Statistics
# ----------------------------------------------------------

print("\nReview Score Statistics")

silver_reviews.select(

    "review_score"

).describe().show()

# ----------------------------------------------------------
# Sample Records
# ----------------------------------------------------------

print("\nSample Records")

silver_reviews.show(

    5,

    truncate=False

)

print("=" * 90)
print("ORDER REVIEWS VALIDATION COMPLETED")
print("=" * 90)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Cell 21:
# ==========================================================
# Geolocation Transformation
# Bronze -> Silver
# ==========================================================

print("=" * 90)
print("GEOLOCATION TRANSFORMATION")
print("=" * 90)

# ----------------------------------------------------------
# Remove Exact Duplicate Records
# ----------------------------------------------------------

silver_geolocation = geolocation_df.dropDuplicates()

print(f"Rows after duplicate removal : {silver_geolocation.count():,}")

# ----------------------------------------------------------
# Identify String Columns
# ----------------------------------------------------------

string_columns = [

    field.name

    for field in silver_geolocation.schema.fields

    if isinstance(field.dataType, StringType)

]

# ----------------------------------------------------------
# Trim String Columns
# ----------------------------------------------------------

for column in string_columns:

    silver_geolocation = silver_geolocation.withColumn(

        column,

        F.trim(F.col(column))

    )

# ----------------------------------------------------------
# Convert Empty Strings to NULL
# ----------------------------------------------------------

for column in string_columns:

    silver_geolocation = silver_geolocation.withColumn(

        column,

        F.when(
            F.col(column) == "",
            F.lit(None)
        ).otherwise(
            F.col(column)
        )

    )

# ----------------------------------------------------------
# Standardize City and State
# ----------------------------------------------------------

silver_geolocation = (

    silver_geolocation

    .withColumn(

        "geolocation_city",

        F.initcap(F.col("geolocation_city"))

    )

    .withColumn(

        "geolocation_state",

        F.upper(F.col("geolocation_state"))

    )

)

# ----------------------------------------------------------
# Cast Numeric Columns
# ----------------------------------------------------------

numeric_columns = {

    "geolocation_zip_code_prefix": "integer",
    "geolocation_lat": "double",
    "geolocation_lng": "double"

}

for column, datatype in numeric_columns.items():

    silver_geolocation = silver_geolocation.withColumn(

        column,

        F.col(column).cast(datatype)

    )

# ----------------------------------------------------------
# Validate Latitude
# ----------------------------------------------------------

silver_geolocation = silver_geolocation.withColumn(

    "geolocation_lat",

    F.when(

        F.col("geolocation_lat").between(-90, 90),

        F.col("geolocation_lat")

    ).otherwise(

        F.lit(None)

    )

)

# ----------------------------------------------------------
# Validate Longitude
# ----------------------------------------------------------

silver_geolocation = silver_geolocation.withColumn(

    "geolocation_lng",

    F.when(

        F.col("geolocation_lng").between(-180, 180),

        F.col("geolocation_lng")

    ).otherwise(

        F.lit(None)

    )

)

# ----------------------------------------------------------
# Add Silver Audit Columns
# ----------------------------------------------------------

silver_geolocation = (

    silver_geolocation

    .withColumn(

        "silver_created_timestamp",

        F.current_timestamp()

    )

    .withColumn(

        "source_layer",

        F.lit("Bronze")

    )

    .withColumn(

        "target_layer",

        F.lit("Silver")

    )

    .withColumn(

        "notebook_name",

        F.lit(NOTEBOOK_NAME)

    )

)

print("✓ Geolocation transformation completed successfully.")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Cell 22:
# ==========================================================
# Geolocation Validation
# ==========================================================

print("=" * 90)
print("GEOLOCATION VALIDATION")
print("=" * 90)

print(f"Rows    : {silver_geolocation.count():,}")
print(f"Columns : {len(silver_geolocation.columns)}")

# ----------------------------------------------------------
# Schema
# ----------------------------------------------------------

print("\nSchema")

silver_geolocation.printSchema()

# ----------------------------------------------------------
# Missing Values
# ----------------------------------------------------------

print("\nMissing Values")

silver_geolocation.select(

    *[

        F.sum(

            F.col(c).isNull().cast("int")

        ).alias(c)

        for c in silver_geolocation.columns

    ]

).show(truncate=False)

# ----------------------------------------------------------
# Latitude Statistics
# ----------------------------------------------------------

print("\nLatitude Statistics")

silver_geolocation.select(

    "geolocation_lat"

).describe().show()

# ----------------------------------------------------------
# Longitude Statistics
# ----------------------------------------------------------

print("\nLongitude Statistics")

silver_geolocation.select(

    "geolocation_lng"

).describe().show()

# ----------------------------------------------------------
# Top States
# ----------------------------------------------------------

print("\nTop States")

silver_geolocation.groupBy(

    "geolocation_state"

).count().orderBy(

    F.desc("count")

).show(10, truncate=False)

# ----------------------------------------------------------
# Sample Records
# ----------------------------------------------------------

print("\nSample Records")

silver_geolocation.show(

    5,

    truncate=False

)

print("=" * 90)
print("GEOLOCATION VALIDATION COMPLETED")
print("=" * 90)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Cell 23:
# ==========================================================
# Save Silver Tables to Lakehouse
# ==========================================================

print("=" * 90)
print("WRITING SILVER TABLES")
print("=" * 90)

silver_tables = {

    "silver_customers": silver_customers,
    "silver_orders": silver_orders,
    "silver_order_items": silver_order_items,
    "silver_products": silver_products,
    "silver_sellers": silver_sellers,
    "silver_payments": silver_payments,
    "silver_reviews": silver_reviews,
    "silver_geolocation": silver_geolocation

}

for table_name, dataframe in silver_tables.items():

    print(f"Writing {table_name}...")

    (

        dataframe.write

        .mode("overwrite")

        .format("delta")

        .saveAsTable(table_name)

    )

    print(f"✓ {table_name} written successfully.")

print()

print("=" * 90)
print("SILVER LAYER COMPLETED SUCCESSFULLY")
print("=" * 90)

print(f"Total Silver Tables Written : {len(silver_tables)}")

print("\nTables Created:")

for table in silver_tables.keys():

    print(f"✓ {table}")

print("\nSilver Layer is ready for Gold transformations.")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
