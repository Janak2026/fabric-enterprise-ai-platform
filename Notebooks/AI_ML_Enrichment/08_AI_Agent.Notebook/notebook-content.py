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

# # 08 — AI Agent
# 
# ## Fabric Enterprise AI Platform
# 
# ### Purpose
# 
# This notebook implements the **Agentic Decision Layer** of Project TITAN.
# 
# The AI Agent consumes curated Business, Machine Learning, and AI intelligence produced by the upstream platform and converts those signals into structured business decisions and recommended actions.
# 
# The objective is not only to describe what happened, but to determine:
# 
# **What happened → What may happen → Why it matters → What should happen next**
# 
# ---
# 
# ## Agent Architecture
# 
# Business Delta Tables  
# ↓  
# Machine Learning Predictions  
# ↓  
# AI Intelligence  
# ↓  
# **Agent Observation Context**  
# ↓  
# **Risk & Opportunity Evaluation**  
# ↓  
# **Decision Engine**  
# ↓  
# **Recommended Actions**  
# ↓  
# Human-in-the-Loop Governance  
# ↓  
# Agent Delta Tables
# 
# ---
# 
# ## Agent Responsibilities
# 
# The agent performs four logical stages:
# 
# 1. **Observe** — combine relevant Business, ML, and AI signals.
# 2. **Reason** — evaluate risk, opportunity, confidence, and supporting evidence.
# 3. **Decide** — assign decision type, risk level, and priority.
# 4. **Act** — generate a recommended business action and determine whether human approval is required.
# 
# ---
# 
# ## Outputs
# 
# ### `agent_decisions`
# 
# Persistent record of agent-generated business decisions, including:
# 
# - Decision ID
# - Entity
# - Decision Type
# - Risk Level
# - Priority
# - Reasoning
# - Recommended Action
# - Confidence Score
# - Decision Timestamp
# 
# ### `agent_action_log`
# 
# Persistent action and governance record containing:
# 
# - Action ID
# - Decision ID
# - Action Type
# - Action Description
# - Action Status
# - Human Approval Requirement
# - Created Timestamp
# 
# ---
# 
# ## Design Principle
# 
# The agent is advisory by default.
# 
# High-impact decisions are routed through **human-in-the-loop approval** rather than being automatically executed.
# 
# The resulting Agent tables become inputs to the `09_Analytics` serving layer and downstream Power BI experience.


# CELL ********************

# Cell 1:
# ==========================================================
# Import Required Libraries
# ==========================================================

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

PROJECT_NAME = "Fabric Enterprise AI Platform"
NOTEBOOK_NAME = "08_AI_Agent"

SOURCE_LAYER = "Business + ML + AI"
TARGET_LAYER = "Agent"

DECISIONS_TABLE = "agent_decisions"
ACTION_LOG_TABLE = "agent_action_log"

print("=" * 70)
print(PROJECT_NAME)
print("=" * 70)

print(f"Notebook       : {NOTEBOOK_NAME}")
print(f"Execution Time : {datetime.now():%Y-%m-%d %H:%M:%S}")
print(f"Source Layer   : {SOURCE_LAYER}")
print(f"Target Layer   : {TARGET_LAYER}")
print("=" * 70)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## Agent Input Contract
# 
# The Agent does not independently recreate Business, ML, or AI calculations.
# 
# It consumes existing curated intelligence from upstream Delta tables.
# 
# For each business entity evaluated by the agent, the observation context should provide:
# 
# - Entity identifier
# - Business performance signal
# - ML risk/prediction signal
# - AI intelligence/sentiment signal
# - Confidence or supporting evidence
# 
# This keeps the Agent layer decoupled from upstream implementation details.

# CELL ********************

# Cell 3:
# ==========================================================
# Load Agent Intelligence
# ==========================================================

BUSINESS_TABLE = "business_models_fact_sales"
ML_TABLE       = "machine_learning_prediction_sales"
AI_TABLE       = "ai_engine_sales_insights"

business_df = spark.table(BUSINESS_TABLE)
ml_df       = spark.table(ML_TABLE)
ai_df       = spark.table(AI_TABLE)

print("=" * 70)
print("Agent Intelligence Sources Loaded")
print("=" * 70)

print(f"Business : {BUSINESS_TABLE}")
print(f"ML       : {ML_TABLE}")
print(f"AI       : {AI_TABLE}")

print("=" * 70)
print("Agent Intelligence Layer : READY")
print("=" * 70)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## Agent Observation Context
# 
# The observation layer creates a unified view of the signals required for decision-making.
# 
# Instead of exposing all upstream columns to the Agent, only decision-relevant features are selected.
# 
# This reduces unnecessary data movement and creates a clear contract between the intelligence layers and the Agent.

# CELL ********************

# Cell 4:
# ==========================================================
# Build Agent Observation Context
# ==========================================================

agent_context_df = (
    business_df.alias("b")
    .join(
        ml_df.alias("m"),
        F.col("b.Calendar_SK") == F.col("m.Calendar_SK"),
        "left"
    )
    .join(
        ai_df.alias("a"),
        F.col("b.Sales_SK") == F.col("a.Sales_SK"),
        "left"
    )
    .select(
        F.col("b.Sales_SK").alias("entity_id"),
        F.col("b.Calendar_SK").alias("calendar_sk"),
        F.col("b.Order_ID").alias("order_id"),
        F.col("b.Order_Status").alias("order_status"),
        F.col("b.Delivery_Status").alias("delivery_status"),
        F.col("b.Delivery_Delay_Days").alias("delivery_delay_days"),
        F.col("b.Total_Item_Value").alias("sales_value"),
        F.col("b.Review_Score").alias("review_score"),
        F.col("m.Actual_Revenue").alias("actual_revenue"),
        F.col("m.Predicted_Revenue").alias("predicted_revenue"),
        F.col("m.Prediction_Error").alias("prediction_error"),
        F.col("a.Sales_Insight").alias("ai_sales_insight")
    )
)

print("=" * 70)
print("Agent Observation Context")
print("=" * 70)

print("Business + ML + AI intelligence integrated successfully")
print("Agent Observation Context : READY")

print("=" * 70)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## Agent Decision Engine
# 
# The Decision Engine evaluates the combined observation context and classifies each entity according to business risk and required intervention.
# 
# Decision outputs are structured rather than free-form so that they can be persisted, governed, queried, and consumed by downstream analytics.
# 
# ### Example
# 
# Declining Business Performance  
# + High ML Risk  
# + Negative AI Signal  
# → HIGH Risk  
# → P1 Priority  
# → Intervention Required

# CELL ********************

# Cell 5:
# ==========================================================
# Agent Decision Logic
# ==========================================================

agent_decisions_df = (
    agent_context_df

    # Unique decision identifier
    .withColumn("decision_id", F.expr("uuid()"))

    # Risk Classification
    .withColumn(
        "risk_level",
        F.when(
            (F.col("delivery_delay_days") > 7) |
            (F.col("review_score") <= 2) |
            (F.abs(F.col("prediction_error")) > 1000),
            "HIGH"
        )
        .when(
            (F.col("delivery_delay_days") > 3) |
            (F.col("review_score") == 3) |
            (F.abs(F.col("prediction_error")) > 500),
            "MEDIUM"
        )
        .otherwise("LOW")
    )

    # Priority
    .withColumn(
        "priority",
        F.when(F.col("risk_level") == "HIGH", "P1")
         .when(F.col("risk_level") == "MEDIUM", "P2")
         .otherwise("P3")
    )

    # Agent Decision
    .withColumn(
        "agent_decision",
        F.when(F.col("risk_level") == "HIGH", "ESCALATE")
         .when(F.col("risk_level") == "MEDIUM", "MONITOR")
         .otherwise("NO_ACTION")
    )

    # Recommended Action
    .withColumn(
        "recommended_action",
        F.when(
            F.col("risk_level") == "HIGH",
            "Immediate business review and corrective action required"
        )
        .when(
            F.col("risk_level") == "MEDIUM",
            "Monitor operational and revenue performance"
        )
        .otherwise("Continue normal monitoring")
    )

    # Agent Metadata
    .withColumn("decision_timestamp", F.current_timestamp())
    .withColumn("agent_name", F.lit("TITAN_Business_Agent"))
)

print("=" * 70)
print("Agent Decision Logic")
print("=" * 70)
print("Risk Classification : COMPLETED")
print("Priority Assignment : COMPLETED")
print("Decision Generation : COMPLETED")
print("Action Generation   : COMPLETED")
print("Decision ID          : GENERATED")
print("=" * 70)
print("Agent Decision Logic : READY")
print("=" * 70)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Cell 6:
# ==========================================================
# Agent Decision Reasoning
# ==========================================================

agent_decisions_df = (
    agent_decisions_df
    .withColumn(
        "decision_reason",

        F.when(
            F.col("risk_level") == "HIGH",
            F.concat_ws(
                " | ",
                F.lit("High-risk condition detected"),

                F.when(
                    F.col("delivery_delay_days") > 7,
                    F.concat(
                        F.lit("Delivery delay: "),
                        F.col("delivery_delay_days").cast("string"),
                        F.lit(" days")
                    )
                ),

                F.when(
                    F.col("review_score") <= 2,
                    F.concat(
                        F.lit("Low review score: "),
                        F.col("review_score").cast("string")
                    )
                ),

                F.when(
                    F.abs(F.col("prediction_error")) > 1000,
                    F.concat(
                        F.lit("High prediction error: "),
                        F.round(
                            F.abs(F.col("prediction_error")), 2
                        ).cast("string")
                    )
                )
            )
        )

        .when(
            F.col("risk_level") == "MEDIUM",
            F.concat_ws(
                " | ",
                F.lit("Moderate-risk condition detected"),

                F.when(
                    F.col("delivery_delay_days") > 3,
                    F.concat(
                        F.lit("Delivery delay: "),
                        F.col("delivery_delay_days").cast("string"),
                        F.lit(" days")
                    )
                ),

                F.when(
                    F.col("review_score") == 3,
                    F.concat(
                        F.lit("Review score: "),
                        F.col("review_score").cast("string")
                    )
                ),

                F.when(
                    F.abs(F.col("prediction_error")) > 500,
                    F.concat(
                        F.lit("Prediction error: "),
                        F.round(
                            F.abs(F.col("prediction_error")), 2
                        ).cast("string")
                    )
                )
            )
        )

        .otherwise(
            "Business and ML indicators are within acceptable thresholds"
        )
    )
)

print("=" * 70)
print("Agent Decision Reasoning")
print("=" * 70)
print("Decision Explanation : COMPLETED")
print("Agent Reasoning       : READY")
print("=" * 70)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## Agent Persistence & Governance
# 
# Agent decisions are persisted as Delta tables to provide:
# 
# - Decision traceability
# - Historical auditability
# - Human approval workflows
# - Downstream analytics
# - Power BI visibility
# - Future agent performance monitoring
# 
# The Agent produces two persistent outputs:
# 
# `agent_decisions`
# 
# `agent_action_log`

# CELL ********************

# Cell 7:
# ==========================================================
# Save Agent Decisions
# ==========================================================

print("=" * 70)
print("Persisting Agent Decisions")
print("=" * 70)

(
    agent_decisions_df
    .write
    .format("delta")
    .mode("overwrite")
    .option("overwriteSchema", "true")
    .saveAsTable(DECISIONS_TABLE)
)

print(f"Delta Table : {DECISIONS_TABLE}")
print("Agent Decisions : PERSISTED")
print("=" * 70)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Cell 8:
# ==========================================================
# Persist Agent Action Log
# ==========================================================

print("=" * 70)
print("Persisting Agent Action Log")
print("=" * 70)

# Read persisted Agent decisions
decisions_df = (
    spark.table(DECISIONS_TABLE)
    .select(
        "decision_id",
        "risk_level",
        "recommended_action"
    )
)

# Generate Agent actions
agent_action_log_df = (
    decisions_df
    .withColumn("action_id", F.expr("uuid()"))
    .withColumn(
        "action_type",
        F.when(F.col("risk_level") == "HIGH", "CORRECTIVE_ACTION")
         .when(F.col("risk_level") == "MEDIUM", "MONITORING")
         .otherwise("STANDARD_MONITORING")
    )
    .withColumn(
        "action_description",
        F.col("recommended_action")
    )
    .withColumn(
        "action_status",
        F.when(F.col("risk_level") == "HIGH", "PENDING_APPROVAL")
         .otherwise("OPEN")
    )
    .withColumn(
        "requires_human_approval",
        F.col("risk_level") == "HIGH"
    )
    .withColumn(
        "created_timestamp",
        F.current_timestamp()
    )
    .select(
        "action_id",
        "decision_id",
        "action_type",
        "action_description",
        "action_status",
        "requires_human_approval",
        "created_timestamp"
    )
)

# Persist Delta table
(
    agent_action_log_df.write
    .format("delta")
    .mode("overwrite")
    .saveAsTable(ACTION_LOG_TABLE)
)

print(f"Delta Table      : {ACTION_LOG_TABLE}")
print("Agent Action Log : PERSISTED")
print("=" * 70)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## Human-in-the-Loop Governance
# 
# The Agent is designed as a governed enterprise decision-support system.
# 
# Low and medium-risk recommendations may be surfaced directly for monitoring.
# 
# High-risk recommendations are marked:
# 
# **PENDING_APPROVAL**
# 
# and:
# 
# **requires_human_approval = true**
# 
# This prevents the Agent from autonomously executing high-impact business actions without human oversight.
# 
# The architecture therefore separates:
# 
# **Reasoning → Recommendation → Approval → Execution**

# CELL ********************

# Cell 9:
# ==========================================================
# Agent Validation & Execution Summary
# ==========================================================

decision_summary = (
    spark.table(DECISIONS_TABLE)
    .groupBy("risk_level")
    .count()
)

action_summary = (
    spark.table(ACTION_LOG_TABLE)
    .groupBy("action_status")
    .count()
)

print("=" * 70)
print("AI AGENT EXECUTION SUMMARY")
print("=" * 70)

print("\nDecision Distribution")
display(decision_summary)

print("\nAction Distribution")
display(action_summary)

print("=" * 70)
print(f"Decision Table : {DECISIONS_TABLE}")
print(f"Action Table   : {ACTION_LOG_TABLE}")
print("Agent Status   : READY")
print("Next Notebook  : 09_Analytics")
print("=" * 70)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
