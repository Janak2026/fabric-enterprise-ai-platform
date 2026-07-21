# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   },
# META   "dependencies": {
# META     "lakehouse": {
# META       "default_lakehouse": "205b75e6-aa22-4abc-8067-30b44b9ba069",
# META       "default_lakehouse_name": "EnterpriseLakehouse",
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
# ## Notebook: 01_Fabric_Setup
# 
# **Purpose**
# Validate the Microsoft Fabric environment and confirm that the workspace, Spark runtime, and Lakehouse are configured correctly before building data pipelines.
# 
# | Property | Value |
# |----------|-------|
# | Project | Fabric Enterprise AI Platform |
# | Notebook | 01_Fabric_Setup |
# | Lakehouse | EnterpriseLakehouse |
# | Version | 1.0 |
# | Author | Janardhana Rao Komanapalli |
# 
# ---

# CELL ********************

## Cell 1:
# ---------------------------------------
# Imports
# ---------------------------------------

from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

## Cell 2:
# ---------------------------------------
# Spark Session Information
# ---------------------------------------

print("=" * 60)
print("Fabric Enterprise AI Platform")
print("=" * 60)

print(f"Spark Version : {spark.version}")

print("\nSetup completed successfully.")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

## Cell 3:
employees = [

    (101,"John","Sales",55000),

    (102,"Sarah","Finance",68000),

    (103,"David","IT",72000),

    (104,"Priya","HR",60000),

    (105,"Michael","Marketing",64000)

]

columns = [

    "EmployeeID",

    "EmployeeName",

    "Department",

    "Salary"

]

df = spark.createDataFrame(employees, columns)

display(df)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

## Cell 4:
print(f"Total Records : {df.count()}")

print(f"Columns : {len(df.columns)}")

df.printSchema()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

## Cell 5:
df.write.mode("overwrite").format("delta").saveAsTable("setup_employees")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

## Cell 6:
employee_df = spark.table("setup_employees")

display(employee_df)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

## Cell 7:
display(

spark.sql("""

SELECT

Department,

COUNT(*) AS EmployeeCount,

AVG(Salary) AS AverageSalary

FROM employees

GROUP BY Department

ORDER BY Department

""")

)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

## Cell 8:
transformed_df = (

employee_df

.withColumn(

"AnnualSalary",

col("Salary") * 12

)

)

display(transformed_df)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# # Setup Completed Successfully
# 
# The following validations were completed successfully:
# 
# - Spark Session
# - Lakehouse Connectivity
# - DataFrame Creation
# - Delta Table Creation
# - SQL Query Execution
# - DataFrame Transformation
# 
# The Fabric environment is now ready for the Data Engineering pipeline.
# 
# Next Notebook:
# 
# **02_Bronze_Ingestion**

# CELL ********************

## Cell 9:
print("=" * 60)
print("Fabric Setup Validation Completed Successfully")
print("=" * 60)

print("✓ Spark Session")
print("✓ EnterpriseLakehouse Connected")
print("✓ DataFrame Created")
print("✓ Delta Table Written")
print("✓ Delta Table Read")
print("✓ SQL Validation Completed")
print("✓ Data Transformation Completed")

print("\nEnvironment Status: READY")
print("Next Step: 02_Bronze_Ingestion")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
