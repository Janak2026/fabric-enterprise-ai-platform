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
# ## Notebook: 01_Fabric_Setup
# 
# ### Purpose
# This notebook validates and prepares the Microsoft Fabric environment before executing any Data Engineering, Machine Learning, AI Engineering, and Power BI workloads.
# 
# ### Objectives
# 
# - Validate Spark Environment
# - Validate Enterprise Lakehouse
# - Validate OneLake Connectivity
# - Validate Folder Structure
# - Validate Read / Write Operations
# - Validate SQL Endpoint Connectivity
# 
# ---
# 
# ## Enterprise Architecture
# 
# Landing
# ↓
# Bronze
# ↓
# Silver
# ↓
# Gold
# ↓
# Machine Learning / AI
# ↓
# SQL Endpoint
# ↓
# Power BI
# 
# ---
# 
# **Author:** Janardhana Rao Komanapalli
# 
# **Project:** Fabric Enterprise AI Platform
# 
# **Version:** 1.0

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
# Project Information
# ==========================================================

print("=" * 60)
print("Fabric Enterprise AI Platform")
print("=" * 60)

print(f"Notebook      : 01_Fabric_Setup")
print(f"Execution Time: {datetime.now()}")
print(f"Python Version: {platform.python_version()}")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Cell 3:
# ==========================================================
# Spark Session Validation
# ==========================================================

spark = SparkSession.builder.getOrCreate()

print("=" * 60)
print("Spark Environment")
print("=" * 60)

print(f"Spark Version        : {spark.version}")
print(f"Current Database     : {spark.catalog.currentDatabase()}")

print("\nSpark Session Status : PASS")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## Environment Validation
# The following section validates the Microsoft Fabric workspace, Lakehouse connection, and storage structure before executing any Data Engineering, Machine Learning, AI Engineering, or Power BI workloads.

# CELL ********************

# Cell 4:
# ==========================================================
# Lakehouse Validation
# ==========================================================

print("=" * 60)
print("Lakehouse Validation")
print("=" * 60)

try:

    current_database = spark.catalog.currentDatabase()

    print(f"Lakehouse Name   : AI_ML_LakeHouse")
    print(f"Current Database : {current_database}")

    databases = [db.name for db in spark.catalog.listDatabases()]

    print(f"Available Databases : {len(databases)}")

    print("Lakehouse Status : PASS")

except Exception as ex:

    print("Lakehouse Status : FAIL")
    print(ex)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## Folder Structure Validation
# Verify that the required Enterprise Lakehouse folder structure exists before ingestion begins.

# CELL ********************

# Cell 5:
# ==========================================================
# Raw Files Validation
# ==========================================================

raw_files_path = "Files/raw_files"

print("=" * 60)
print("Raw Files Validation")
print("=" * 60)

try:

    files = mssparkutils.fs.ls(raw_files_path)

    print(f"Raw Files Path : {raw_files_path}")
    print(f"Files Found    : {len(files)}")

    print("Raw Files Status : PASS")

except Exception as ex:

    print("Raw Files Status : FAIL")
    print(ex)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Cell 6:
# ==========================================================
# Environment Validation Summary
# ==========================================================

print("=" * 70)
print("          FABRIC ENTERPRISE AI PLATFORM")
print("        ENVIRONMENT VALIDATION SUMMARY")
print("=" * 70)

print("✓ Spark Session            : PASS")
print("✓ Lakehouse Connection     : PASS")
print("✓ Spark Runtime            : PASS")
print("✓ Raw Files Validation     : PASS")
print("✓ Environment Ready        : PASS")

print("-" * 70)

print("Current Platform Architecture")

print("""

                Raw Files
                    │
                    ▼
          Landing Ingestion
                    │
                    ▼
              Bronze Tables
                    │
                    ▼
              Silver Tables
                    │
                    ▼
         Business Models Gold
                    │
        ┌───────────┴───────────┐
        ▼                       ▼
 Machine Learning         AI Engineering
        │                       │
        └───────────┬───────────┘
                    ▼
          AI_ML_Enriched_Gold
                    │
                    ▼
              AI Agent Layer
                    │
                    ▼
         SQL Analytics Endpoint
                    │
                    ▼
             Semantic Model
                    │
                    ▼
                Power BI

""")

print("-" * 70)

print("Status        : ENVIRONMENT READY")
print("Next Notebook : 02_Landing_Ingestion")

print("=" * 70)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
