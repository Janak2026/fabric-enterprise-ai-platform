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

# # 09 — Enterprise Analytics Layer
# 
# ## Project TITAN — Fabric Enterprise AI Platform
# 
# ### Purpose
# 
# This notebook creates the final analytics-serving layer of Project TITAN by integrating curated outputs from the Business, Machine Learning, AI Engineering, and AI Agent layers.
# 
# The Analytics layer converts platform intelligence into consumption-ready datasets designed for enterprise reporting, operational monitoring, and Power BI.
# 
# ### Source Layers
# 
# - Business Models
# - Machine Learning
# - AI Engineering
# - AI Agent
# 
# ### Analytics Outputs
# 
# This notebook persists three Delta tables:
# 
# 1. **analytics_executive_summary**
#    - Enterprise business KPIs
#    - Revenue performance
#    - ML prediction indicators
#    - Executive-level analytical metrics
# 
# 2. **analytics_risk_monitoring**
#    - Agent risk classifications
#    - Decision priorities
#    - Recommended actions
#    - Human approval requirements
#    - Operational monitoring information
# 
# 3. **analytics_ai_insights**
#    - AI-generated sales insights
#    - Business context
#    - Explainable intelligence for downstream analytics
# 
# ### Architecture
# 
# Business Models + Machine Learning + AI Engineering + AI Agent  
# ↓  
# Enterprise Analytics Layer  
# ↓  
# Analytics Delta Tables  
# ↓  
# SQL Analytics Endpoint  
# ↓  
# Semantic Model  
# ↓  
# Power BI
# 
# ### Objective
# 
# Provide a governed and optimized analytical serving layer that exposes business performance, predictive intelligence, AI-generated insights, and agentic decisions through a unified enterprise analytics model.


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
NOTEBOOK_NAME = "09_Analytics"

SOURCE_LAYERS = "Business + ML + AI + Agent"
TARGET_LAYER = "Analytics"

# Analytics Delta Tables
EXECUTIVE_SUMMARY_TABLE = "analytics_executive_summary"
RISK_MONITORING_TABLE = "analytics_risk_monitoring"
AI_INSIGHTS_TABLE = "analytics_ai_insights"

print("=" * 70)
print(PROJECT_NAME)
print("=" * 70)

print(f"Notebook       : {NOTEBOOK_NAME}")
print(f"Execution Time : {datetime.now():%Y-%m-%d %H:%M:%S}")
print(f"Source Layers  : {SOURCE_LAYERS}")
print(f"Target Layer   : {TARGET_LAYER}")

print("-" * 70)

print("Analytics Outputs")
print(f"Executive      : {EXECUTIVE_SUMMARY_TABLE}")
print(f"Risk           : {RISK_MONITORING_TABLE}")
print(f"AI Insights    : {AI_INSIGHTS_TABLE}")

print("=" * 70)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Cell 3:
# ==========================================================
# Analytics Source Configuration
# ==========================================================

BUSINESS_TABLE = "business_models_fact_sales"
ML_TABLE = "machine_learning_prediction_sales"
AI_TABLE = "ai_engine_sales_insights"
AGENT_DECISIONS_TABLE = "agent_decisions"
AGENT_ACTION_LOG_TABLE = "agent_action_log"

source_tables = {
    "Business"        : BUSINESS_TABLE,
    "Machine Learning": ML_TABLE,
    "AI Engine"       : AI_TABLE,
    "Agent Decisions" : AGENT_DECISIONS_TABLE,
    "Agent Actions"   : AGENT_ACTION_LOG_TABLE
}

print("=" * 70)
print("Analytics Source Configuration")
print("=" * 70)

for source, table_name in source_tables.items():
    print(f"{source:<18} : {table_name}")

print("=" * 70)
print("Analytics Source Configuration : READY")
print("=" * 70)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## Executive Analytics
# 
# The Executive Analytics layer consolidates core business performance and Machine Learning forecast signals into an executive-level analytical dataset.
# 
# ### Primary Inputs
# 
# - `business_models_fact_sales`
# - `machine_learning_prediction_sales`
# 
# ### Analytical Grain
# 
# The dataset is aggregated at the **Calendar level**, aligning the business sales fact with the existing ML sales prediction grain.
# 
# ### Key Measures
# 
# - Total Revenue
# - Total Orders
# - Total Items
# - Average Order Value
# - Average Review Score
# - Average Delivery Delay
# - Actual Revenue
# - Predicted Revenue
# - Prediction Error
# - Revenue Variance
# - Forecast Performance
# 
# ### Output
# 
# `analytics_executive_summary`
# 
# This table is designed for downstream consumption through the SQL Analytics Endpoint, Semantic Model, and Power BI executive reporting.

# CELL ********************

# Cell 4:
# ==========================================================
# Build Executive Analytics
# ==========================================================

# Aggregate business metrics to ML prediction grain
business_summary_df = (
    spark.table(BUSINESS_TABLE)
    .groupBy("Calendar_SK")
    .agg(
        F.sum("Total_Item_Value").alias("total_revenue"),
        F.countDistinct("Order_ID").alias("total_orders"),
        F.count("Sales_SK").alias("total_items"),
        F.avg("Review_Score").alias("avg_review_score"),
        F.avg("Delivery_Delay_Days").alias("avg_delivery_delay_days")
    )
    .withColumn(
        "avg_order_value",
        F.when(
            F.col("total_orders") > 0,
            F.col("total_revenue") / F.col("total_orders")
        ).otherwise(F.lit(0.0))
    )
)

# ML predictions already exist at Calendar_SK grain
ml_summary_df = (
    spark.table(ML_TABLE)
    .select(
        "Calendar_SK",
        "Actual_Revenue",
        "Predicted_Revenue",
        "Prediction_Error"
    )
)

# Integrate Business + ML analytics
executive_summary_df = (
    business_summary_df.alias("b")
    .join(
        ml_summary_df.alias("m"),
        F.col("b.Calendar_SK") == F.col("m.Calendar_SK"),
        "left"
    )
    .select(
        F.col("b.Calendar_SK").alias("calendar_sk"),
        F.col("b.total_revenue"),
        F.col("b.total_orders"),
        F.col("b.total_items"),
        F.round("b.avg_order_value", 2).alias("avg_order_value"),
        F.round("b.avg_review_score", 2).alias("avg_review_score"),
        F.round("b.avg_delivery_delay_days", 2).alias("avg_delivery_delay_days"),
        F.col("m.Actual_Revenue").alias("actual_revenue"),
        F.col("m.Predicted_Revenue").alias("predicted_revenue"),
        F.col("m.Prediction_Error").alias("prediction_error"),
        (
            F.col("m.Predicted_Revenue") -
            F.col("m.Actual_Revenue")
        ).alias("revenue_variance")
    )
)

print("=" * 70)
print("Executive Analytics")
print("=" * 70)
print("Business Aggregation : COMPLETED")
print("ML Integration       : COMPLETED")
print("Executive Analytics  : READY")
print("=" * 70)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Cell 5:
# ==========================================================
# Persist Executive Analytics
# ==========================================================

print("=" * 70)
print("Persisting Executive Analytics")
print("=" * 70)

(
    executive_summary_df
    .write
    .format("delta")
    .mode("overwrite")
    .option("overwriteSchema", "true")
    .saveAsTable(EXECUTIVE_SUMMARY_TABLE)
)

print(f"Delta Table         : {EXECUTIVE_SUMMARY_TABLE}")
print("Executive Analytics : PERSISTED")
print("=" * 70)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## Risk Monitoring Analytics
# 
# The Risk Monitoring layer converts AI Agent decisions and action records into an analytics-ready dataset for operational governance and Power BI monitoring.
# 
# ### Primary Inputs
# 
# - `agent_decisions`
# - `agent_action_log`
# 
# ### Analytical Purpose
# 
# This dataset provides visibility into:
# 
# - Agent risk classification
# - Decision priority
# - Agent-generated decisions
# - Decision reasoning
# - Recommended actions
# - Action status
# - Human approval requirements
# 
# ### Governance Flow
# 
# Agent Decision  
# ↓  
# Risk Classification  
# ↓  
# Recommended Action  
# ↓  
# Human Approval Requirement  
# ↓  
# Action Status  
# ↓  
# Risk Monitoring Analytics
# 
# ### Output
# 
# `analytics_risk_monitoring`
# 
# This table provides the serving layer for operational risk dashboards, agent decision monitoring, and human-in-the-loop governance.

# CELL ********************

# Cell 6:
# ==========================================================
# Build Risk Monitoring Analytics
# ==========================================================

decisions_df = (
    spark.table(AGENT_DECISIONS_TABLE)
    .select(
        "decision_id",
        "entity_id",
        "order_id",
        "risk_level",
        "priority",
        "agent_decision",
        "recommended_action",
        "decision_reason",
        "decision_timestamp",
        "agent_name"
    )
)

actions_df = (
    spark.table(AGENT_ACTION_LOG_TABLE)
    .select(
        "decision_id",
        "action_id",
        "action_type",
        "action_status",
        "requires_human_approval",
        "created_timestamp"
    )
)

risk_monitoring_df = (
    decisions_df.alias("d")
    .join(
        actions_df.alias("a"),
        F.col("d.decision_id") == F.col("a.decision_id"),
        "left"
    )
    .select(
        F.col("d.decision_id"),
        F.col("a.action_id"),
        F.col("d.entity_id"),
        F.col("d.order_id"),
        F.col("d.risk_level"),
        F.col("d.priority"),
        F.col("d.agent_decision"),
        F.col("d.recommended_action"),
        F.col("d.decision_reason"),
        F.col("a.action_type"),
        F.col("a.action_status"),
        F.col("a.requires_human_approval"),
        F.col("d.decision_timestamp"),
        F.col("a.created_timestamp").alias("action_created_timestamp"),
        F.col("d.agent_name")
    )
)

print("=" * 70)
print("Risk Monitoring Analytics")
print("=" * 70)
print("Agent Decisions Integration : COMPLETED")
print("Agent Actions Integration   : COMPLETED")
print("Risk Monitoring Analytics   : READY")
print("=" * 70)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Cell 7:
# ==========================================================
# Persist Risk Monitoring Analytics
# ==========================================================

print("=" * 70)
print("Persisting Risk Monitoring Analytics")
print("=" * 70)

(
    risk_monitoring_df
    .write
    .format("delta")
    .mode("overwrite")
    .option("overwriteSchema", "true")
    .saveAsTable(RISK_MONITORING_TABLE)
)

print(f"Delta Table              : {RISK_MONITORING_TABLE}")
print("Risk Monitoring Analytics : PERSISTED")
print("=" * 70)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## AI Insights Analytics
# 
# The AI Insights Analytics layer combines AI-generated sales intelligence with business context to create a reporting-ready dataset for downstream analytics.
# 
# ### Primary Inputs
# 
# - `ai_engine_sales_insights`
# - `business_models_fact_sales`
# 
# ### Analytical Purpose
# 
# This layer enables analysis of:
# 
# - AI-generated sales insights
# - Sales and revenue context
# - Delivery performance
# - Customer review performance
# - AI model lineage
# - Prompt lineage
# - Insight generation timestamps
# 
# ### Integration Key
# 
# `Sales_SK`
# 
# ### Output
# 
# `analytics_ai_insights`
# 
# This table provides the AI-enriched serving layer for Power BI reporting and downstream enterprise analytics.

# CELL ********************

# Cell 8:
# ==========================================================
# Build AI Insights Analytics
# ==========================================================

business_ai_context_df = (
    spark.table(BUSINESS_TABLE)
    .select(
        "Sales_SK",
        "Order_ID",
        "Customer_SK",
        "Product_SK",
        "Seller_SK",
        "Calendar_SK",
        "Order_Status",
        "Delivery_Status",
        "Delivery_Delay_Days",
        "Total_Item_Value",
        "Review_Score"
    )
)

ai_insights_source_df = (
    spark.table(AI_TABLE)
    .select(
        "Sales_SK",
        "Sales_Insight",
        "Model_Name",
        "Prompt_Name",
        "Generated_Timestamp"
    )
)

analytics_ai_insights_df = (
    ai_insights_source_df.alias("a")
    .join(
        business_ai_context_df.alias("b"),
        F.col("a.Sales_SK") == F.col("b.Sales_SK"),
        "left"
    )
    .select(
        F.col("a.Sales_SK").alias("sales_sk"),
        F.col("b.Order_ID").alias("order_id"),
        F.col("b.Customer_SK").alias("customer_sk"),
        F.col("b.Product_SK").alias("product_sk"),
        F.col("b.Seller_SK").alias("seller_sk"),
        F.col("b.Calendar_SK").alias("calendar_sk"),
        F.col("b.Order_Status").alias("order_status"),
        F.col("b.Delivery_Status").alias("delivery_status"),
        F.col("b.Delivery_Delay_Days").alias("delivery_delay_days"),
        F.col("b.Total_Item_Value").alias("total_item_value"),
        F.col("b.Review_Score").alias("review_score"),
        F.col("a.Sales_Insight").alias("sales_insight"),
        F.col("a.Model_Name").alias("model_name"),
        F.col("a.Prompt_Name").alias("prompt_name"),
        F.col("a.Generated_Timestamp").alias("generated_timestamp")
    )
)

print("=" * 70)
print("AI Insights Analytics")
print("=" * 70)

print("Business Context Integration : COMPLETED")
print("AI Insights Integration      : COMPLETED")
print("AI Insights Analytics        : READY")

print("=" * 70)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Cell 9:
# ==========================================================
# Persist AI Insights Analytics
# ==========================================================

print("=" * 70)
print("Persisting AI Insights Analytics")
print("=" * 70)

(
    analytics_ai_insights_df
    .write
    .format("delta")
    .mode("overwrite")
    .option("overwriteSchema", "true")
    .saveAsTable(AI_INSIGHTS_TABLE)
)

print(f"Delta Table          : {AI_INSIGHTS_TABLE}")
print("AI Insights Analytics : PERSISTED")
print("=" * 70)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Cell 10:
# ==========================================================
# Analytics Validation & Completion Summary
# ==========================================================

analytics_tables = [
    EXECUTIVE_SUMMARY_TABLE,
    RISK_MONITORING_TABLE,
    AI_INSIGHTS_TABLE
]

print("=" * 80)
print("ANALYTICS LAYER VALIDATION")
print("=" * 80)

for table_name in analytics_tables:
    if not spark.catalog.tableExists(table_name):
        raise RuntimeError(f"Required analytics table missing: {table_name}")

    print(f"PASS : {table_name}")

print("=" * 80)
print("ANALYTICS PROCESSING COMPLETED SUCCESSFULLY")
print("=" * 80)

print(f"Analytics Tables : {len(analytics_tables)}")
print("Executive Layer  : READY")
print("Risk Monitoring  : READY")
print("AI Insights      : READY")
print("Analytics Status : READY")

print("=" * 80)
print("09_Analytics : COMPLETED")
print("=" * 80)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
