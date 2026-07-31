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

# OneLake Paths
LANDING_PATH = "Files/Landing"
BRONZE_DATABASE = "dbo"

print("=" * 70)
print(PROJECT_NAME)
print("=" * 70)

print(f"Notebook          : {NOTEBOOK_NAME}")
print(f"Execution Time    : {datetime.now():%Y-%m-%d %H:%M:%S}")
print(f"Landing Layer     : {LANDING_PATH}")
print(f"Bronze Database   : {BRONZE_DATABASE}")

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
# ==========================================================
# Dataset Configuration
# ==========================================================

datasets = {

    "bronze_customers"      : "Customers",
    "bronze_orders"         : "Orders",
    "bronze_order_items"    : "OrderItems",
    "bronze_order_payments" : "Payments",
    "bronze_products"       : "Products",
    "bronze_sellers"        : "Sellers",
    "bronze_order_reviews"  : "Reviews",
    "bronze_geolocation"    : "Geolocation"

}

print("=" * 70)
print("Bronze Dataset Configuration")
print("=" * 70)

print(f"Landing Source : {LANDING_PATH}")
print("-" * 70)

for table_name, landing_folder in datasets.items():

    print(f"{table_name:<25} -> {landing_folder}")

print("-" * 70)
print(f"Bronze Tables Configured : {len(datasets)}")
print("Configuration Status     : READY")
print("=" * 70)

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

for table_name, landing_folder in datasets.items():

    landing_path = f"{LANDING_PATH}/{landing_folder}"

    print(f"\nReading : {landing_folder}")
    print("-" * 70)

    try:

        df = (
            spark.read
                 .parquet(landing_path)
        )

        landing_dataframes[table_name] = df

        row_count = df.count()
        column_count = len(df.columns)

        print(f"Landing Path : {landing_path}")
        print(f"Rows         : {row_count:,}")
        print(f"Columns      : {column_count}")
        print("Status       : PASS")

    except Exception as ex:

        print(f"Landing Path : {landing_path}")
        print("Status       : FAIL")
        print(ex)

print("=" * 70)
print("Landing Layer Read Completed")
print("=" * 70)

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
        .withColumn("source_dataset", F.lit(datasets[table_name]))
        .withColumn("source_layer", F.lit("Landing"))
        .withColumn("target_layer", F.lit("Bronze"))
        .withColumn("created_by", F.lit(NOTEBOOK_NAME))
        .withColumn("load_date", F.current_date())
    )

    bronze_dataframes[table_name] = df

    print(f"✓ {table_name:<25} Audit Columns Added")

print("=" * 70)
print(f"Bronze DataFrames Prepared : {len(bronze_dataframes)}")
print("Audit Enrichment Status    : READY")
print("=" * 70)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Cell 6:
# ==========================================================
# Save Bronze Delta Tables
# ==========================================================

print("=" * 70)
print("Creating Bronze Delta Tables")
print("=" * 70)

tables_created = 0

for table_name, df in bronze_dataframes.items():

    print(f"\nCreating : {table_name}")
    print("-" * 70)

    try:

        (
            df.write
              .mode("overwrite")
              .format("delta")
              .saveAsTable(table_name)
        )

        row_count = df.count()

        tables_created += 1

        print(f"Table Name : {table_name}")
        print(f"Rows       : {row_count:,}")
        print("Status     : PASS")

    except Exception as ex:

        print(f"Table Name : {table_name}")
        print("Status     : FAIL")
        print(ex)

print("=" * 70)
print(f"Bronze Tables Created : {tables_created}/{len(bronze_dataframes)}")
print("Bronze Layer Status   : READY")
print("=" * 70)

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
print("BRONZE TABLE VALIDATION")
print("=" * 90)

for table_name in datasets.keys():

    try:

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

        print(f"✓ {table_name:<25} Rows: {row_count:,}")

    except Exception as ex:

        summary.append(
            (
                table_name,
                0,
                0,
                "FAIL"
            )
        )

        print(f"✗ {table_name:<25} FAILED")
        print(ex)

print("=" * 90)
print(f"Bronze Tables Validated : {len(summary)}")
print("=" * 90)

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
    ["Table", "Rows", "Columns", "Status"]
)

display(summary_df)

print("=" * 90)
print("BRONZE PROCESSING COMPLETED SUCCESSFULLY")
print("=" * 90)

print(f"Tables Created : {len(summary)}")
print(f"Total Rows     : {sum(item[1] for item in summary):,}")
print("Bronze Status  : READY")
print("Next Notebook  : 04_Silver_Transformation")

print("=" * 90)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
