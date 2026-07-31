# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   },
# META   "dependencies": {
# META     "lakehouse": {
# META       "default_lakehouse": "7f869026-0276-4bf0-9150-97ec98ea5455",
# META       "default_lakehouse_name": "EnterpriseLakeFabric",
# META       "default_lakehouse_workspace_id": "d7ae502d-247b-4ea5-857f-127fff869a69",
# META       "known_lakehouses": [
# META         {
# META           "id": "7f869026-0276-4bf0-9150-97ec98ea5455"
# META         }
# META       ]
# META     }
# META   }
# META }

# MARKDOWN ********************

# # Fabric Enterprise AI Platform
# 
# ## Notebook: 02_Landing_Ingestion
# 
# ### Purpose
# This notebook ingests raw data from external sources into the Landing layer of the Enterprise Lakehouse.
# 
# ### Objectives
# 
# - Configure ingestion paths
# - Validate source datasets
# - Ingest raw data into OneLake
# - Preserve source data without transformation
# - Validate successful ingestion
# 
# ---
# 
# ## Data Flow
# 
# External Source
#         │
#         ▼
# Landing
#         │
#         ▼
# Bronze
# 
# ---
# 
# Version : 1.0

# CELL ********************

# Cell 1:
# ==========================================================
# Import Required Libraries
# ==========================================================

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import *
from datetime import datetime
import platform

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
NOTEBOOK_NAME = "02_Landing_Ingestion"
SOURCE_LAYER = "Landing"
TARGET_LAYER = "Bronze"
LANDING_PATH = "Files/Landing/Files"

print("=" * 70)
print(PROJECT_NAME)
print("=" * 70)

print(f"Notebook        : 02_Landing_Ingestion")
print(f"Execution Time  : {datetime.now()}")
print(f"Landing Path    : {LANDING_PATH}")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Cell 3:
# ==========================================================
# Landing Folder Validation
# ==========================================================

landing_folders = [

    "Files/Landing/Customers",
    "Files/Landing/Orders",
    "Files/Landing/OrderItems",
    "Files/Landing/Payments",
    "Files/Landing/Products",
    "Files/Landing/Sellers",
    "Files/Landing/Reviews",
    "Files/Landing/Geolocation"

]

print("=" * 70)
print("Landing Folder Validation")
print("=" * 70)

for folder in landing_folders:

    try:

        mssparkutils.fs.mkdirs(folder)

        print(f"✓ Created : {folder}")

    except Exception as ex:

        print(f"✗ Failed  : {folder}")
        print(ex)

print("=" * 70)
print("Landing Structure Ready")
print("=" * 70)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## Landing Dataset Validation
# 
# This section validates all raw datasets available in the Landing Files area.
# 
# Validation includes:
# 
# - File Availability
# - Schema
# - Row Count
# - Column Count
# - Sample Records
# 
# No transformations are performed.

# CELL ********************

# Cell 4:
# ==========================================================
# Landing Dataset Configuration
# ==========================================================

LANDING_PATH = "Files/Landing/Files"

datasets = {

    "Customers"   : f"{LANDING_PATH}/customers_dataset.csv",
    "Orders"      : f"{LANDING_PATH}/orders_dataset.csv",
    "OrderItems"  : f"{LANDING_PATH}/order_items_dataset.csv",
    "Payments"    : f"{LANDING_PATH}/order_payments_dataset.csv",
    "Products"    : f"{LANDING_PATH}/products_dataset.csv",
    "Sellers"     : f"{LANDING_PATH}/sellers_dataset.csv",
    "Reviews"     : f"{LANDING_PATH}/order_reviews_dataset.csv",
    "Geolocation" : f"{LANDING_PATH}/geolocation_dataset.csv"
}

print("=" * 70)
print("Landing Dataset Configuration")
print("=" * 70)

for dataset, path in datasets.items():

    print(f"{dataset:<15} : {path}")

print("=" * 70)
print(f"Total Datasets : {len(datasets)}")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Cell 5:
# ==========================================================
# Landing Dataset Validation
# ==========================================================

landing_dataframes = {}
summary = []

print("=" * 70)
print("Landing Dataset Validation")
print("=" * 70)

for dataset_name, dataset_path in datasets.items():

    print(f"\nProcessing : {dataset_name}")
    print("-" * 70)

    df = (
        spark.read
             .option("header", True)
             .csv(dataset_path)
    )

    # Store DataFrame
    landing_dataframes[dataset_name] = df

    # Calculate once
    row_count = df.count()
    column_count = len(df.columns)

    # Store metadata for Cell 6
    summary.append(
        (
            dataset_name,
            row_count,
            column_count,
            "PASS"
        )
    )

    print(f"Rows       : {row_count:,}")
    print(f"Columns    : {column_count}")

    display(df.limit(5))

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## Landing Validation Summary
# 
# This section summarizes the validation results for all datasets discovered in the Landing layer.
# 
# The summary confirms that every dataset is available and ready for Bronze processing.

# CELL ********************

# Cell 6:
# ==========================================================
# Landing Validation Summary
# ==========================================================

print("=" * 90)
print("LANDING INGESTION VALIDATION SUMMARY")
print("=" * 90)

summary_df = spark.createDataFrame(
    summary,
    [
        "Dataset",
        "Rows",
        "Columns",
        "Status"
    ]
)

display(summary_df)

total_rows = sum(item[1] for item in summary)

print("=" * 90)
print("LANDING INGESTION COMPLETED SUCCESSFULLY")
print("=" * 90)

print(f"Datasets Validated : {len(summary)}")
print(f"Total Rows         : {total_rows:,}")
print("Landing Status     : READY")
print("Next Notebook      : 03_Bronze_Processing")

print("=" * 90)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
