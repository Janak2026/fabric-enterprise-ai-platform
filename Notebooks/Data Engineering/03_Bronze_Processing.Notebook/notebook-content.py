# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   },
# META   "dependencies": {
# META     "lakehouse": {
# META       "default_lakehouse": "205b75e6-aa22-4abc-8067-30b44b9ba069",
# META       "default_lakehouse_name": "FabricEnterpriseLake",
# META       "default_lakehouse_workspace_id": "91a249b4-c43c-4f8f-a4ce-afd3bf9990df",
# META       "known_lakehouses": [
# META         {
# META           "id": "205b75e6-aa22-4abc-8067-30b44b9ba069"
# META         }
# META       ]
# META     }
# META   }
# META }

# MARKDOWN ********************

# # Fabric Enterprise AI Platform
# 
# ## Notebook: 03_Bronze_Processing
# 
# ### Purpose
# 
# This notebook converts raw Landing datasets into Bronze Delta tables.
# 
# The Bronze layer preserves the original data while providing a performant Delta format for downstream processing.
# 
# ### Objectives
# 
# - Read Landing datasets
# - Preserve original schema
# - Add ingestion metadata
# - Write Bronze Delta tables
# - Validate successful table creation
# 
# ---
# 
# ## Data Flow
# 
# Landing (CSV)
#       │
#       ▼
# Bronze (Delta)

# CELL ********************

# Cell 1:
# ==========================================================
# Import Required Libraries
# ==========================================================

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
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

spark = SparkSession.builder.getOrCreate()

PROJECT_NAME = "Fabric Enterprise AI Platform"

NOTEBOOK_NAME = "03_Bronze_Processing"

LANDING_PATH = "Files/Landing/Files"

BRONZE_PATH = "Tables"

print("=" * 70)
print(PROJECT_NAME)
print("=" * 70)

print(f"Notebook        : {NOTEBOOK_NAME}")
print(f"Execution Time  : {datetime.now()}")
print(f"Landing Path    : {LANDING_PATH}")
print(f"Bronze Layer    : {BRONZE_PATH}")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Cell 3:
# ==========================================================
# Dataset Configuration
# ==========================================================

datasets = {

    "bronze_customers"       : "customers_dataset.csv",
    "bronze_orders"          : "orders_dataset.csv",
    "bronze_order_items"     : "order_items_dataset.csv",
    "bronze_order_payments"  : "order_payments_dataset.csv",
    "bronze_products"        : "products_dataset.csv",
    "bronze_sellers"         : "sellers_dataset.csv",
    "bronze_order_reviews"   : "order_reviews_dataset.csv",
    "bronze_geolocation"     : "geolocation_dataset.csv"

}

print("=" * 70)
print("Bronze Dataset Configuration")
print("=" * 70)

for table_name, file_name in datasets.items():

    print(f"{table_name:<25} -> {file_name}")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## Landing Dataset Processing
# 
# This section reads all Landing CSV files and prepares them for the Bronze layer.
# 
# Processing performed:
# 
# - Read Landing CSV
# - Preserve Original Schema
# - Add Audit Columns
# - Write Bronze Delta Tables
# 
# No business transformations are applied.

# CELL ********************

# Cell 4:
# ==========================================================
# Read Landing Datasets
# ==========================================================

landing_dataframes = {}

print("=" * 70)
print("Reading Landing Datasets")
print("=" * 70)

for table_name, file_name in datasets.items():

    file_path = f"{LANDING_PATH}/{file_name}"

    print(f"\nReading : {file_name}")

    df = (
        spark.read
             .option("header", True)
             .csv(file_path)
    )

    landing_dataframes[table_name] = df

    print(f"Rows    : {df.count():,}")
    print(f"Columns : {len(df.columns)}")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## Bronze Layer Processing
# 
# This section converts all Landing datasets into Bronze Delta tables.
# 
# Processing performed:
# 
# - Read Landing CSV files
# - Preserve original schema
# - Add audit metadata
# - Write managed Delta tables
# - Validate successful table creation
# 
# No business transformations are applied.

# CELL ********************

# Cell 5:
# ==========================================================
# Add Bronze Audit Columns
# ==========================================================

bronze_dataframes = {}

print("=" * 70)
print("Adding Bronze Audit Columns")
print("=" * 70)

for table_name, df in landing_dataframes.items():

    df = (
        df
        .withColumn("ingestion_timestamp", F.current_timestamp())
        .withColumn("source_file", F.lit(datasets[table_name]))
        .withColumn("source_layer", F.lit("Landing"))
        .withColumn("created_by", F.lit(NOTEBOOK_NAME))
        .withColumn("load_date", F.current_date())
    )

    bronze_dataframes[table_name] = df

    print(f"✓ {table_name}")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Cell 6:
# ==========================================================
# Create Bronze Delta Tables
# ==========================================================

print("=" * 70)
print("Creating Bronze Delta Tables")
print("=" * 70)

for table_name, df in bronze_dataframes.items():

    print(f"Creating : {table_name}")

    (
        df.write
          .mode("overwrite")
          .format("delta")
          .saveAsTable(table_name)
    )

    print(f"✓ {table_name} created")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Cell 7:
# ==========================================================
# Bronze Table Validation
# ==========================================================

summary = []

print("=" * 90)
print("Bronze Table Validation")
print("=" * 90)

for table_name in datasets.keys():

    df = spark.table(table_name)

    row_count = df.count()
    column_count = len(df.columns)

    summary.append(
        (
            table_name,
            row_count,
            column_count,
            "PASS"
        )
    )

    print(f"✓ {table_name}")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Cell 8:
# ==========================================================
# Bronze Processing Summary
# ==========================================================

summary_df = spark.createDataFrame(
    summary,
    [
        "Table",
        "Rows",
        "Columns",
        "Status"
    ]
)

display(summary_df)

total_rows = sum(item[1] for item in summary)

print("=" * 90)
print("BRONZE PROCESSING COMPLETED SUCCESSFULLY")
print("=" * 90)

print(f"Tables Created : {len(summary)}")
print(f"Total Rows     : {total_rows:,}")
print("Bronze Status  : READY")
print("Next Notebook  : 04_Silver_Transformation")

print("=" * 90)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
