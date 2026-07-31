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

# OneLake Paths
RAW_PATH = "Files/raw_files"
LANDING_PATH = "Files/Landing"

print("=" * 70)
print(PROJECT_NAME)
print("=" * 70)

print(f"Notebook         : {NOTEBOOK_NAME}")
print(f"Execution Time   : {datetime.now():%Y-%m-%d %H:%M:%S}")
print(f"Raw Files Path   : {RAW_PATH}")
print(f"Landing Path     : {LANDING_PATH}")

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

validated = 0

for folder in landing_folders:

    try:

        mssparkutils.fs.mkdirs(folder)

        validated += 1

        print(f"✓ Ready : {folder}")

    except Exception as ex:

        print(f"✗ Failed: {folder}")
        print(ex)

print("=" * 70)
print(f"Landing Folders Verified : {validated}/{len(landing_folders)}")
print("Landing Layer Status     : READY")
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

datasets = {

    "Customers"   : f"{RAW_PATH}/customers_dataset.csv",
    "Orders"      : f"{RAW_PATH}/orders_dataset.csv",
    "OrderItems"  : f"{RAW_PATH}/order_items_dataset.csv",
    "Payments"    : f"{RAW_PATH}/order_payments_dataset.csv",
    "Products"    : f"{RAW_PATH}/products_dataset.csv",
    "Sellers"     : f"{RAW_PATH}/sellers_dataset.csv",
    "Reviews"     : f"{RAW_PATH}/order_reviews_dataset.csv",
    "Geolocation" : f"{RAW_PATH}/geolocation_dataset.csv"

}

print("=" * 70)
print("Landing Dataset Configuration")
print("=" * 70)

print(f"Source Folder : {RAW_PATH}")
print("-" * 70)

for dataset_name, dataset_path in datasets.items():

    print(f"{dataset_name:<15} : {dataset_path}")

print("-" * 70)
print(f"Datasets Configured : {len(datasets)}")
print("Configuration Status : READY")
print("=" * 70)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Cell 5:
# ==========================================================
# Landing Dataset Validation & Ingestion
# ==========================================================

landing_dataframes = {}
summary = []

print("=" * 70)
print("Landing Dataset Validation & Ingestion")
print("=" * 70)

for dataset_name, dataset_path in datasets.items():

    print(f"\nProcessing : {dataset_name}")
    print("-" * 70)

    try:

        # -----------------------------------------------
        # Read Source Dataset
        # -----------------------------------------------
        df = (
            spark.read
                 .option("header", True)
                 .csv(dataset_path)
        )

        # -----------------------------------------------
        # Save to Landing Layer
        # -----------------------------------------------
        landing_output_path = f"{LANDING_PATH}/{dataset_name}"

        (
            df.write
              .mode("overwrite")
              .parquet(landing_output_path)
        )

        # -----------------------------------------------
        # Store DataFrame
        # -----------------------------------------------
        landing_dataframes[dataset_name] = df

        row_count = df.count()
        column_count = len(df.columns)

        summary.append(
            (
                dataset_name,
                row_count,
                column_count,
                "PASS"
            )
        )

        print(f"Source Path      : {dataset_path}")
        print(f"Landing Path     : {landing_output_path}")
        print(f"Rows             : {row_count:,}")
        print(f"Columns          : {column_count}")
        print("Status           : PASS")

        display(df.limit(5))

    except Exception as ex:

        summary.append(
            (
                dataset_name,
                0,
                0,
                "FAIL"
            )
        )

        print(f"Source Path      : {dataset_path}")
        print("Status           : FAIL")
        print(ex)

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
# Landing Ingestion Summary
# ==========================================================

print("=" * 90)
print("LANDING INGESTION SUMMARY")
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

# ----------------------------------------------------------
# Overall Statistics
# ----------------------------------------------------------

total_datasets = len(summary)
total_rows = sum(item[1] for item in summary)

passed = sum(1 for item in summary if item[3] == "PASS")
failed = sum(1 for item in summary if item[3] == "FAIL")

print("=" * 90)

if failed == 0:
    print("LANDING INGESTION COMPLETED SUCCESSFULLY")
else:
    print("LANDING INGESTION COMPLETED WITH ERRORS")

print("=" * 90)

print(f"Datasets Processed : {total_datasets}")
print(f"Successful         : {passed}")
print(f"Failed             : {failed}")
print(f"Total Rows Loaded  : {total_rows:,}")
print(f"Landing Location   : {LANDING_PATH}")

if failed == 0:
    print("Landing Status     : READY")
else:
    print("Landing Status     : REVIEW REQUIRED")

print("Next Notebook      : 03_Bronze_Processing")

print("=" * 90)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
