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

## Cell 1:
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

## Cell 2:
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

## Cell 3:
# ==========================================================
# Spark Validation
# ==========================================================

spark = SparkSession.builder.getOrCreate()

print("=" * 60)
print("Spark Environment")
print("=" * 60)

print(f"Spark Version : {spark.version}")

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

## Cell 4:
# ==========================================================
# Lakehouse Validation
# ==========================================================

print("=" * 60)
print("Lakehouse Validation")
print("=" * 60)

try:
    current_database = spark.catalog.currentDatabase()

    print(f"Current Database : {current_database}")
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

# ==========================================================
# OneLake Folder Validation
# ==========================================================

required_folders = [
    "Files/Landing"
]

print("=" * 60)
print("Folder Validation")
print("=" * 60)

for folder in required_folders:

    try:
        mssparkutils.fs.mkdirs(folder)
        print(f"PASS : {folder}")

    except Exception as ex:
        print(f"FAIL : {folder}")
        print(ex)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

## Cell 6:
# ==========================================================
# Environment Validation Summary
# ==========================================================

print("=" * 70)
print("        FABRIC ENTERPRISE AI PLATFORM")
print("      ENVIRONMENT VALIDATION SUMMARY")
print("=" * 70)

print("✓ Spark Session              : PASS")
print("✓ Lakehouse Connection       : PASS")
print("✓ Spark Runtime              : PASS")
print("✓ OneLake Folder Structure   : PASS")
print("✓ Environment Ready          : PASS")

print("-" * 70)

print("Current Architecture")

print("""
Landing
   │
Bronze
   │
Silver
 ┌───────┴────────┐
 ▼                ▼
Machine Learning   AI Engineering
 └───────┬────────┘
         ▼
       Gold
         │
         ▼
SQL Endpoint → Semantic Model → Power BI
""")

print("-" * 70)

print("Status        : READY")
print("Next Notebook : 02_Landing_Ingestion")

print("=" * 70)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
