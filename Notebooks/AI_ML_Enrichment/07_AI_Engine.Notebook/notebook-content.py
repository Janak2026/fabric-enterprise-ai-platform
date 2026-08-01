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

# # 07 - AI Engine
# 
# ## Objective
# 
# This notebook transforms business-ready and machine learning datasets into AI-ready knowledge assets for enterprise AI applications.
# 
# The generated AI assets will serve as the foundation for semantic search, Retrieval-Augmented Generation (RAG), AI Agents, intelligent recommendations, and conversational analytics.
# 
# ---
# 
# ## Source Tables
# 
# ### Business Models
# 
# - business_models_dim_product
# - business_models_dim_customer
# - business_models_fact_sales
# 
# ### Machine Learning
# 
# - machine_learning_feature_product
# - machine_learning_feature_customer
# - machine_learning_prediction_sales
# 
# ---
# 
# ## Output Tables
# 
# - AI_Engineering_product_embeddings
# - AI_Engineering_customer_embeddings
# - AI_Engineering_sales_insights
# - AI_Engineering_recommendations
# - AI_Engineering_prompt_repository
# 
# ---
# 
# ## AI Engineering Workflow
# 
# ```text
# Read Business & ML Tables
#             │
#             ▼
#  Prepare AI Input Data
#             │
#             ▼
#  Generate AI Knowledge Assets
#             │
#             ├── Product Embeddings
#             ├── Customer Embeddings
#             ├── Sales Insights
#             ├── Recommendations
#             └── Prompt Repository
#             │
#             ▼
#  Store AI Delta Tables
#             │
#             ▼
#  Validate Outputs
# ```
# 
# ---
# 
# ## Business Purpose
# 
# The AI Engineering layer enriches structured enterprise data with Generative AI capabilities by producing reusable AI assets that power:
# 
# - Semantic Search
# - Retrieval-Augmented Generation (RAG)
# - AI Agents
# - Intelligent Recommendations
# - Conversational Analytics
# - AI-Powered Power BI Experiences


# CELL ********************

# Cell 0:
# ============================================================
# Install Required Libraries
# ============================================================

# %pip install --upgrade openai

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Cell 1:
# ============================================================
# Azure OpenAI Configuration
# ============================================================

# Azure OpenAI Resource Endpoint
AZURE_OPENAI_ENDPOINT = "https://aoai-titan.openai.azure.com/"

# Azure OpenAI API Key
AZURE_OPENAI_API_KEY = "KEY"

# Azure OpenAI API Version (Chat Completions)
AZURE_OPENAI_API_VERSION = "2024-02-01"

# Azure OpenAI API Version (Responses API)
AZURE_OPENAI_API_VERSION_RESPONSES = "2025-03-01-preview"

# Azure OpenAI Deployment Name
EMBEDDING_DEPLOYMENT = "text-embedding-3-small"

# Chat Deployment
CHAT_DEPLOYMENT = "gpt-5-mini"

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Cell 2:
# ============================================================
# Imports
# ============================================================

# Standard Python Libraries
from datetime import datetime
import time
from pprint import pprint

# Azure OpenAI
from openai import AzureOpenAI

# PySpark Functions
from pyspark.sql.functions import (
    col,
    lit,
    concat,
    concat_ws,
    round,
    current_timestamp,
    when,
    count,
    avg,
    explode,
    format_number,
    collect_list
)

# PySpark Data Types
from pyspark.sql.types import (
    StructType,
    StructField,
    StringType,
    IntegerType,
    DoubleType,
    BooleanType,
    FloatType,
    ArrayType,
    TimestampType
)


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Cell 3:
# ============================================================
# Helper Functions
# ============================================================

def validate_table(df, table_name, sample_rows=5):
    """
    Display basic validation information for a Spark DataFrame.

    Parameters:
        df (DataFrame): Spark DataFrame
        table_name (str): Name of the table
        sample_rows (int): Number of sample rows to display
    """

    print("=" * 80)
    print(f"Table Name : {table_name}")
    print(f"Record Count : {df.count():,}")
    print("=" * 80)

    print("\nSchema:")
    df.printSchema()

    print("\nSample Data:")
    display(df.limit(sample_rows))

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Cell 4:
# ============================================================
# Read Source Tables
# ============================================================

print("Loading Business Models...")

dim_product_df = spark.read.table("Business_Models_dim_product")
dim_customer_df = spark.read.table("Business_Models_dim_customer")
fact_sales_df = spark.read.table("Business_Models_fact_sales")

print("Loading Machine Learning Tables...")

feature_product_df = spark.read.table("Machine_Learning_feature_product")
feature_customer_df = spark.read.table("Machine_Learning_feature_customer")
prediction_sales_df = spark.read.table("Machine_Learning_prediction_sales")

print("All source tables loaded successfully.")

# ============================================================
# Validate Source Tables
# ============================================================

validate_table(dim_product_df, "Business_Models_dim_product")

validate_table(dim_customer_df, "Business_Models_dim_customer")

validate_table(fact_sales_df, "Business_Models_fact_sales" )

validate_table(feature_product_df, "Machine_Learning_feature_product")

validate_table(feature_customer_df, "Machine_Learning_feature_customer")

validate_table(prediction_sales_df, "Machine_Learning_prediction_sales")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# # Product Embeddings
# 
# ## Objective
# 
# Generate semantic vector embeddings for each product by combining business attributes into descriptive text and converting that text into high-dimensional embedding vectors using Azure OpenAI.
# 
# The generated embeddings will enable semantic search, similarity matching, Retrieval-Augmented Generation (RAG), and AI-powered product recommendations.
# 
# ---
# 
# ### Input Tables
# 
# - Business_Models_dim_product
# - Machine_Learning_feature_product
# 
# ### Output Table
# 
# - AI_Engineering_product_embeddings

# CELL ********************

# Cell 5:
# ============================================================
# Create Product Embedding Text
# ============================================================

print("Creating product embedding text...")

product_embedding_text_df = (
    feature_product_df
    .join(
        dim_product_df.select(
            "Product_SK",
            "Product_Category"
        ),
        on="Product_SK",
        how="inner"
    )
    .select(
        "Product_SK",
        concat_ws(
            " | ",
            concat(lit("Category: "), col("Product_Category")),
            concat(lit("Average Selling Price: "), round(col("Average_Selling_Price"), 2)),
            concat(lit("Average Freight Cost: "), round(col("Average_Freight_Cost"), 2)),
            concat(lit("Average Review Score: "), round(col("Average_Review_Score"), 2)),
            concat(lit("Total Orders: "), col("Total_Orders")),
            concat(lit("Total Revenue: "), round(col("Total_Revenue"), 2))
        ).alias("Product_Text")
    )
)

print("Product embedding text created successfully.")

# ============================================================
# Validate Product Embedding Text
# ============================================================

validate_table(
    product_embedding_text_df,
    "Product Embedding Text"
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Cell 6:
# ============================================================
# Generate Product Embeddings
# ============================================================

print("Generating product embeddings...")

# ============================================================
# Azure OpenAI Client
# ============================================================

client = AzureOpenAI(
    azure_endpoint=AZURE_OPENAI_ENDPOINT,
    api_key=AZURE_OPENAI_API_KEY,
    api_version=AZURE_OPENAI_API_VERSION
)

# ============================================================
# Configuration
# ============================================================

BATCH_SIZE = 500

created_date = datetime.now()

total_products = product_embedding_text_df.count()

print(f"Total Products : {total_products:,}")
print(f"Batch Size     : {BATCH_SIZE}")
print("=" * 60)

# ============================================================
# Generate Embeddings
# ============================================================

product_embeddings = []

batch = []

processed_products = 0
api_calls = 0

for row in product_embedding_text_df.toLocalIterator():

    batch.append(row)

    if len(batch) == BATCH_SIZE:

        texts = [r.Product_Text for r in batch]

        while True:

            try:

                response = client.embeddings.create(
                    model=EMBEDDING_DEPLOYMENT,
                    input=texts
                )

                api_calls += 1

                for record, embedding in zip(batch, response.data):

                    product_embeddings.append(
                        (
                            record.Product_SK,
                            record.Product_Text,
                            embedding.embedding,
                            created_date
                        )
                    )

                processed_products += len(batch)

                print(
                    f"Processed {processed_products:,} / {total_products:,} products"
                )

                batch = []

                break

            except Exception as ex:

                print(f"Batch failed. Retrying in 5 seconds...\n{ex}")

                time.sleep(5)

# ============================================================
# Process Remaining Records
# ============================================================

if batch:

    response = client.embeddings.create(
        model=EMBEDDING_DEPLOYMENT,
        input=[r.Product_Text for r in batch]
    )

    api_calls += 1

    for record, embedding in zip(batch, response.data):

        product_embeddings.append(
            (
                record.Product_SK,
                record.Product_Text,
                embedding.embedding,
                created_date
            )
        )

    processed_products += len(batch)

    print(
        f"Processed {processed_products:,} / {total_products:,} products"
    )

# ============================================================
# Create Spark DataFrame
# ============================================================

product_embeddings_df = spark.createDataFrame(
    product_embeddings,
    [
        "Product_SK",
        "Product_Text",
        "Embedding_Vector",
        "Created_Date"
    ]
)

print("=" * 60)
print("Product embeddings generated successfully.")
print(f"Total Products : {processed_products:,}")
print(f"API Calls      : {api_calls:,}")
print("=" * 60)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Cell 7:
# ============================================================
# Save Product Embeddings
# ============================================================

print("Saving product embeddings...")

(
    product_embeddings_df
    .write
    .format("delta")
    .mode("overwrite")
    .saveAsTable("AI_Engine_product_embeddings")
)

print("Product embeddings saved successfully.")

# ============================================================
# Validate Product Embeddings
# ============================================================

ai_product_embeddings_df = spark.read.table(
    "AI_Engine_product_embeddings"
)

validate_table(
    ai_product_embeddings_df,
    "AI_Engine_product_embeddings"
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Cell 8:
# ============================================================
# Create Customer Embedding Text
# ============================================================

print("Creating customer embedding text...")

customer_embedding_text_df = (
    feature_customer_df
    .join(
        dim_customer_df.select(
            "Customer_SK",
            "Customer_State"
        ),
        on="Customer_SK",
        how="inner"
    )
    .select(
        "Customer_SK",
        concat_ws(
            " | ",
            concat(lit("Customer State: "), col("Customer_State")),
            concat(lit("Total Orders: "), col("Total_Orders")),
            concat(lit("Total Items Purchased: "), col("Total_Items_Purchased")),
            concat(lit("Total Spend: "), round(col("Total_Spent"), 2)),
            concat(lit("Average Order Value: "), round(col("Average_Order_Value"), 2)),
            concat(lit("Preferred Payment Type: "), col("Preferred_Payment_Type")),
            concat(lit("Average Delivery Days: "), round(col("Average_Delivery_Days"), 2)),
            concat(lit("On-Time Delivery Rate: "), round(col("On_Time_Delivery_Rate"), 2)),
            concat(lit("Average Review Score: "), round(col("Average_Review_Score"), 2)),
            concat(lit("Customer Lifetime (Days): "), col("Customer_Lifetime_Days"))
        ).alias("Customer_Text")
    )
)

print("Customer embedding text created successfully.")

# ============================================================
# Validate Customer Embedding Text
# ============================================================

validate_table(
    customer_embedding_text_df,
    "Customer Embedding Text"
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Cell 9:
# ============================================================
# Generate Customer Embeddings
# ============================================================

print("Generating customer embeddings...")

# ============================================================
# Azure OpenAI Client
# ============================================================

client = AzureOpenAI(
    azure_endpoint=AZURE_OPENAI_ENDPOINT,
    api_key=AZURE_OPENAI_API_KEY,
    api_version=AZURE_OPENAI_API_VERSION
)

# ============================================================
# Configuration
# ============================================================

BATCH_SIZE = 500

created_date = datetime.now()

customer_embedding_text_df = customer_embedding_text_df.cache()

total_customers = customer_embedding_text_df.count()

print(f"Total Customers : {total_customers:,}")
print(f"Batch Size      : {BATCH_SIZE}")
print("=" * 60)

processed_customers = 0
api_calls = 0
batch = []
first_batch = True

# ============================================================
# Generate Embeddings & Save Batch
# ============================================================

for row in customer_embedding_text_df.toLocalIterator():

    batch.append(row)

    if len(batch) == BATCH_SIZE:

        while True:

            try:

                response = client.embeddings.create(
                    model=EMBEDDING_DEPLOYMENT,
                    input=[r.Customer_Text for r in batch]
                )

                api_calls += 1

                batch_records = []

                for record, embedding in zip(batch, response.data):

                    batch_records.append(
                        (
                            record.Customer_SK,
                            record.Customer_Text,
                            embedding.embedding,
                            created_date
                        )
                    )

                batch_df = spark.createDataFrame(
                    batch_records,
                    [
                        "Customer_SK",
                        "Customer_Text",
                        "Embedding_Vector",
                        "Created_Date"
                    ]
                )

                (
                    batch_df.write
                    .format("delta")
                    .mode("overwrite" if first_batch else "append")
                    .saveAsTable("AI_Engine_customer_embeddings")
                )

                first_batch = False

                processed_customers += len(batch)

                print(
                    f"Processed {processed_customers:,} / {total_customers:,} customers"
                )

                batch.clear()

                break

            except Exception as ex:

                print(f"Batch failed. Retrying in 5 seconds...\n{ex}")

                time.sleep(5)

# ============================================================
# Process Remaining Records
# ============================================================

if batch:

    while True:

        try:

            response = client.embeddings.create(
                model=EMBEDDING_DEPLOYMENT,
                input=[r.Customer_Text for r in batch]
            )

            api_calls += 1

            batch_records = []

            for record, embedding in zip(batch, response.data):

                batch_records.append(
                    (
                        record.Customer_SK,
                        record.Customer_Text,
                        embedding.embedding,
                        created_date
                    )
                )

            batch_df = spark.createDataFrame(
                batch_records,
                [
                    "Customer_SK",
                    "Customer_Text",
                    "Embedding_Vector",
                    "Created_Date"
                ]
            )

            (
                batch_df.write
                .format("delta")
                .mode("append")
                .saveAsTable("AI_Engine_customer_embeddings")
            )

            processed_customers += len(batch)

            print(
                f"Processed {processed_customers:,} / {total_customers:,} customers"
            )

            break

        except Exception as ex:

            print(f"Batch failed. Retrying in 5 seconds...\n{ex}")

            time.sleep(5)

print("=" * 60)
print("Customer embeddings generated successfully.")
print(f"Total Customers : {processed_customers:,}")
print(f"API Calls       : {api_calls:,}")
print("=" * 60)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Cell 10:
# ============================================================
# Validate Customer Embeddings
# ============================================================

ai_customer_embeddings_df = spark.read.table(
    "AI_Engine_customer_embeddings"
)

validate_table(
    ai_customer_embeddings_df,
    "AI_Engine_customer_embeddings"
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# # AI Prompt Repository
# 
# ## Objective
# 
# Create a centralized repository of reusable AI prompts used across the AI Engine.
# 
# The prompt repository stores prompt templates, model configuration, and execution parameters, enabling prompt versioning, centralized management, and consistent AI responses across business insight generation, recommendations, and future AI use cases.
# 
# ---
# 
# ### Output Table
# 
# - AI_Engine_prompt_repository

# CELL ********************

# Cell 11:
# ============================================================
# Create Prompt Repository
# ============================================================

print("Creating AI Prompt Repository...")

created_date = datetime.now()

prompt_repository_data = [

    (
        1,
        "Sales Performance Insight",
        "Sales",
        "System",
        """You are an expert retail business analyst.
Analyze the following sales data and provide:
1. Key insights
2. Business risks
3. Opportunities
4. Recommended actions

Sales Data:
{sales_data}""",
        CHAT_DEPLOYMENT,
        0.20,
        1000,
        True,
        "1.0",
        created_date
    ),

    (
        2,
        "Customer Recommendation",
        "Customer",
        "System",
        """Recommend the most relevant products for the customer based on purchasing behaviour and customer profile.

Customer Information:
{customer_data}""",
        CHAT_DEPLOYMENT,
        0.30,
        800,
        True,
        "1.0",
        created_date
    ),

    (
        3,
        "Product Recommendation",
        "Product",
        "System",
        """Explain why the recommended products are suitable for the customer.

Product Information:
{product_data}""",
        CHAT_DEPLOYMENT,
        0.30,
        800,
        True,
        "1.0",
        created_date
    ),

    (
        4,
        "Executive Summary",
        "Executive",
        "System",
        """Generate an executive summary highlighting key business performance, risks and opportunities.

Business Data:
{business_data}""",
        CHAT_DEPLOYMENT,
        0.20,
        1000,
        True,
        "1.0",
        created_date
    ),

    (
        5,
        "Customer Segmentation",
        "Customer",
        "System",
        """Analyze customer behaviour and classify customers into meaningful business segments.

Customer Data:
{customer_data}""",
        CHAT_DEPLOYMENT,
        0.30,
        800,
        True,
        "1.0",
        created_date
    ),

    (
        6,
        "Product Summary",
        "Product",
        "System",
        """Summarize the product performance, strengths and potential business opportunities.

Product Data:
{product_data}""",
        CHAT_DEPLOYMENT,
        0.20,
        800,
        True,
        "1.0",
        created_date
    ),

    (
        7,
        "Inventory Optimization",
        "Inventory",
        "System",
        """Analyze inventory levels and recommend optimization opportunities.

Inventory Data:
{inventory_data}""",
        CHAT_DEPLOYMENT,
        0.20,
        800,
        True,
        "1.0",
        created_date
    ),

    (
        8,
        "Market Trend Analysis",
        "Market",
        "System",
        """Identify significant market trends and provide actionable business recommendations.

Market Data:
{market_data}""",
        CHAT_DEPLOYMENT,
        0.30,
        1000,
        True,
        "1.0",
        created_date
    ),

    (
        9,
        "Customer Churn Risk",
        "Customer",
        "System",
        """Identify customers with high churn risk and recommend retention strategies.

Customer Data:
{customer_data}""",
        CHAT_DEPLOYMENT,
        0.30,
        800,
        True,
        "1.0",
        created_date
    ),

    (
        10,
        "Demand Forecast Explanation",
        "Forecast",
        "System",
        """Explain the forecasted demand trends and business implications.

Forecast Data:
{forecast_data}""",
        CHAT_DEPLOYMENT,
        0.20,
        1000,
        True,
        "1.0",
        created_date
    ),

    (
        11,
        "Sales Recommendation",
        "Sales",
        "Recommendation",
        """You are an expert retail business consultant.

    Based on the following AI-generated sales insight, generate practical, prioritized and actionable business recommendations.

    Your recommendations should focus on:

    1. Immediate Actions
    2. Short-Term Improvements
    3. Long-Term Strategic Initiatives

    Consider the following business objectives:

    - Revenue Growth
    - Customer Retention
    - Cross-Selling & Upselling
    - Inventory Optimization
    - Operational Efficiency
    - Risk Mitigation

    Sales Insight:
    {sales_insight}""",
        CHAT_DEPLOYMENT,
        0.30,
        500,
        True,
        "1.0",
        created_date
    )

]

schema = StructType([
    StructField("Prompt_ID", IntegerType(), False),
    StructField("Prompt_Name", StringType(), False),
    StructField("Prompt_Category", StringType(), False),
    StructField("Prompt_Type", StringType(), False),
    StructField("Prompt_Template", StringType(), False),
    StructField("Model_Name", StringType(), False),
    StructField("Temperature", DoubleType(), False),
    StructField("Max_Tokens", IntegerType(), False),
    StructField("Is_Active", BooleanType(), False),
    StructField("Version", StringType(), False),
    StructField("Created_Date", TimestampType(), False)
])

prompt_repository_df = spark.createDataFrame(
    prompt_repository_data,
    schema
)

print("AI Prompt Repository created successfully.")

validate_table(
    prompt_repository_df,
    "AI Prompt Repository"
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Cell 12:
# ============================================================
# Save Prompt Repository
# ============================================================

print("Saving AI Prompt Repository...")

(
    prompt_repository_df
    .write
    .format("delta")
    .mode("overwrite")
    .saveAsTable("AI_Engine_prompt_repository")
)

print(f"Total Prompts : {prompt_repository_df.count()}")

# ============================================================
# Validate Prompt Repository
# ============================================================

ai_prompt_repository_df = spark.read.table(
    "AI_Engine_prompt_repository"
)

validate_table(
    ai_prompt_repository_df,
    "AI_Engine_prompt_repository"
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# # AI Engine – Sales Insights
# 
# ## Overview
# 
# This section generates AI-powered sales insights by combining sales transactions,
# customer information, and product information into a business context that is
# submitted to Azure OpenAI.
# 
# The generated insights provide natural-language business recommendations,
# risks, opportunities, and performance summaries for each sales transaction.
# 
# ## Source Tables
# 
# - Business_Models_fact_sales
# - Business_Models_dim_customer
# - Business_Models_dim_product
# - AI_Engine_prompt_repository
# 
# ## Target Table
# 
# - AI_Engine_sales_insights
# 
# ## Process Flow
# 
# Fact Sales
#         +
# Customer
#         +
# Product
#         +
# Prompt Repository
#         ↓
# Build Business Context
#         ↓
# Azure OpenAI
#         ↓
# Generate Sales Insight
#         ↓
# Save Delta Table

# CELL ********************

# Cell 13:
# ============================================================
# Read Source Tables
# ============================================================

print("Reading source tables...")

fact_sales_df = spark.read.table(
    "Business_Models_fact_sales"
)

customer_df = spark.read.table(
    "Business_Models_dim_customer"
)

product_df = spark.read.table(
    "Business_Models_dim_product"
)

prompt_df = spark.read.table(
    "AI_Engine_prompt_repository"
)

print("Source tables loaded successfully.")

# ============================================================
# Prepare Sales Context
# ============================================================

print("Preparing sales context...")

sales_context_df = (

    fact_sales_df.alias("f")

    .join(
        customer_df.alias("c"),
        col("f.Customer_SK") == col("c.Customer_SK"),
        "inner"
    )

    .join(
        product_df.alias("p"),
        col("f.Product_SK") == col("p.Product_SK"),
        "inner"
    )

    .select(

        col("f.Sales_SK"),

        concat_ws(

            "\n",

            concat(
                lit("Customer City : "),
                col("c.Customer_City")
            ),

            concat(
                lit("Customer State : "),
                col("c.Customer_State")
            ),

            concat(
                lit("Product Category : "),
                col("p.Product_Category")
            ),

            concat(
                lit("Order Status : "),
                col("f.Order_Status")
            ),

            concat(
                lit("Payment Type : "),
                col("f.Payment_Type")
            ),

            concat(
                lit("Payment Value : "),
                format_number(col("f.Payment_Value"),2)
            ),

            concat(
                lit("Review Score : "),
                col("f.Review_Score")
            ),

            concat(
                lit("Delivery Status : "),
                col("f.Delivery_Status")
            ),

            concat(
                lit("Delivery Delay Days : "),
                col("f.Delivery_Delay_Days")
            )

        ).alias("Sales_Context")

    )

)

print("Sales Context prepared successfully.")

validate_table(
    sales_context_df,
    "Sales Context"
)


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Cell 14:
# ============================================================
# Read Sales Insight Prompt
# ============================================================

print("Reading Sales Insight prompt...")

sales_prompt_df = (
    prompt_df
    .filter(col("Prompt_Name") == "Sales Performance Insight")
    .filter(col("Is_Active") == True)
)

sales_prompt = (
    sales_prompt_df
    .select("Prompt_Template")
    .first()["Prompt_Template"]
)

temperature = (
    sales_prompt_df
    .select("Temperature")
    .first()["Temperature"]
)

max_tokens = (
    sales_prompt_df
    .select("Max_Tokens")
    .first()["Max_Tokens"]
)

print("=" * 60)
print("Sales Insight Prompt Loaded Successfully")
print(f"Temperature : {temperature}")
print(f"Max Tokens  : {max_tokens}")
print("=" * 60)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Cell 15:
# ============================================================
# Test Azure OpenAI Responses API
# ============================================================

from pyspark.sql.functions import *

print("=" * 70)
print("Azure OpenAI Responses API Test")
print("=" * 70)

# ============================================================
# Azure OpenAI Client
# ============================================================

client = AzureOpenAI(
    azure_endpoint=AZURE_OPENAI_ENDPOINT,
    api_key=AZURE_OPENAI_API_KEY,
    api_version=AZURE_OPENAI_API_VERSION_RESPONSES
)

print("Azure OpenAI Client Created Successfully.")

# ============================================================
# Read First Sales Record
# ============================================================

print("\nReading first sales record...")

row = sales_context_df.first()

if row is None:
    raise Exception("Sales Context is empty.")

print(f"Sales_SK : {row.Sales_SK}")

# ============================================================
# Prepare Prompt
# ============================================================

print("\nPreparing prompt...")

prompt = sales_prompt.replace(
    "{sales_data}",
    str(row.Sales_Context)
)

# ============================================================
# Convert everything to native Python types
# ============================================================

chat_model = str(CHAT_DEPLOYMENT)
prompt_text = str(prompt)
max_output = int(max_tokens)

print("\nParameter Validation")
print("-" * 70)

print(f"Model                 : {chat_model}")
print(f"Model Type            : {type(chat_model)}")

print(f"Prompt Type           : {type(prompt_text)}")
print(f"Prompt Length         : {len(prompt_text)}")

print(f"Max Output Tokens     : {max_output}")
print(f"Max Output Type       : {type(max_output)}")

print("-" * 70)

# ============================================================
# Call Azure OpenAI
# ============================================================

print("\nCalling Azure OpenAI...")

import builtins

start_time = time.time()

try:

    response = client.responses.create(

        model=chat_model,

        input=[

            {
                "role": "system",
                "content": [
                    {
                        "type": "input_text",
                        "text": "You are an expert retail business analyst."
                    }
                ]
            },

            {
                "role": "user",
                "content": [
                    {
                        "type": "input_text",
                        "text": prompt_text
                    }
                ]
            }

        ],

        max_output_tokens=max_output

    )

    elapsed_time = builtins.round(
        time.time() - start_time,
        2
    )

except Exception as ex:

    print("\nAzure OpenAI Request Failed")
    print(type(ex))
    print(ex)
    raise

# ============================================================
# Response Summary
# ============================================================

print("\n" + "=" * 70)
print("Response Received")
print("=" * 70)

print(type(response))

# ============================================================
# Raw Response
# ============================================================

print("\nRaw Response")
print("-" * 70)

try:
    pprint(response.model_dump())
except Exception:
    pprint(response)

# ============================================================
# Output Text
# ============================================================

print("\nGenerated Insight")
print("-" * 70)

try:

    print(response.output_text)

except Exception:

    try:

        for item in response.output:
            pprint(item)

    except Exception as ex:

        print(ex)

# ============================================================
# Execution Summary
# ============================================================

print("\n" + "=" * 70)
print("Execution Summary")
print("=" * 70)

print(f"Sales_SK        : {row.Sales_SK}")
print(f"Elapsed Time    : {elapsed_time} seconds")
print("=" * 70)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Cell 16:
# ============================================================
# Generate AI Sales Insights
# ============================================================

print("=" * 70)
print("Generate AI Sales Insights")
print("=" * 70)

# ============================================================
# Read Sales Records
# ============================================================

# NOTE:
# During development we used:
sales_rows = sales_context_df.limit(500).collect()
print(f"Total Records to Process : {len(sales_rows)}")

# Production
# sales_rows = sales_context_df.collect()
# print(f"Total Records to Process : {len(sales_rows)}")

# ============================================================
# Initialize Results
# ============================================================

results = []

# ============================================================
# Process Each Sales Record
# ============================================================

for index, row in enumerate(sales_rows, start=1):

    print("-" * 70)
    print(f"Processing Record {index} of {len(sales_rows)}")
    print(f"Sales_SK : {row.Sales_SK}")

    try:

        # ---------------------------------------------
        # Prepare Prompt
        # ---------------------------------------------

        prompt = sales_prompt.replace(
            "{sales_data}",
            str(row.Sales_Context)
        )

        # ---------------------------------------------
        # Call Azure OpenAI
        # ---------------------------------------------

        response = client.responses.create(

            model=str(CHAT_DEPLOYMENT),

            input=[

                {
                    "role": "system",
                    "content": [
                        {
                            "type": "input_text",
                            "text": "You are an expert retail business analyst."
                        }
                    ]
                },

                {
                    "role": "user",
                    "content": [
                        {
                            "type": "input_text",
                            "text": str(prompt)
                        }
                    ]
                }

            ],

            max_output_tokens=int(max_tokens)

        )

        # ---------------------------------------------
        # Extract Insight
        # ---------------------------------------------

        insight = response.output_text

        if insight is None:
            insight = ""

        print("Status : Success")

    except Exception as ex:

        print("Status : Failed")
        print(ex)

        insight = f"Generation Failed : {str(ex)}"

    # ---------------------------------------------
    # Store Result
    # ---------------------------------------------

    results.append(

        (
            int(row.Sales_SK),
            insight,
            str(CHAT_DEPLOYMENT),
            "Sales Performance Insight",
            datetime.now()
        )

    )

# ============================================================
# Create Spark DataFrame
# ============================================================

print("=" * 70)
print("Creating Spark DataFrame...")
print("=" * 70)

schema = StructType([

    StructField("Sales_SK", IntegerType(), False),

    StructField("Sales_Insight", StringType(), True),

    StructField("Model_Name", StringType(), True),

    StructField("Prompt_Name", StringType(), True),

    StructField("Generated_Timestamp", TimestampType(), True)

])

sales_insight_df = spark.createDataFrame(
    results,
    schema
)

print("Spark DataFrame Created Successfully.")

# ============================================================
# Validate Output
# ============================================================

validate_table(
    sales_insight_df,
    "AI Sales Insights"
)

# ============================================================
# Save Delta Table
# ============================================================

print("=" * 70)
print("Saving Delta Table...")
print("=" * 70)

(
    sales_insight_df
    .write
    .format("delta")
    .mode("overwrite")
    .saveAsTable("AI_Engine_sales_insights")
)

print("AI_Engine_sales_insights Saved Successfully.")

print("=" * 70)
print("Notebook 07_AI_Engine - Cell 16 Completed")
print("=" * 70)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# # AI Recommendations
# 
# ## Objective
# 
# Generate AI-powered business recommendations based on the business insights generated in the previous step.
# 
# Unlike Sales Insights, which explain **what happened** and **why**, Recommendations focus on **what the business should do next**.
# 
# The recommendations are generated using Azure OpenAI GPT-5-mini and stored in the AI recommendation layer.
# 
# ---
# 
# ## Input
# 
# - AI_Engine_sales_insights
# - AI_Engine_prompt_repository
# 
# ---
# 
# ## Process
# 
# 1. Read AI Sales Insights
# 2. Read Recommendation Prompt
# 3. Generate AI Recommendations using Azure OpenAI
# 4. Create Spark DataFrame
# 5. Save Delta Table
# 
# ---
# 
# ## Output
# 
# **Delta Table**
# 
# AI_Engine_recommendations
# 
# ---
# 
# ## Architecture
# 
# Business Models
# 
# ↓
# 
# Sales Context
# 
# ↓
# 
# AI Sales Insights
# 
# ↓
# 
# AI Recommendations
# 
# ↓
# 
# Power BI / AI Agent / Business Applications

# CELL ********************

# Cell 17:
# ============================================================
# Read AI Sales Insights & Recommendation Prompt
# ============================================================

print("=" * 70)
print("Read AI Sales Insights")
print("=" * 70)

# ============================================================
# Read AI Sales Insights
# ============================================================

print("Reading AI Sales Insights...")

sales_insight_df = spark.read.table(
    "AI_Engine_sales_insights"
)

print("AI Sales Insights loaded successfully.")

# ============================================================
# Read Sales Recommendation Prompt
# ============================================================

print("Reading Sales Recommendation Prompt...")

recommendation_prompt_df = (

    prompt_df

    .filter(
        col("Prompt_Name") == "Sales Recommendation"
    )

    .filter(
        col("Is_Active") == True
    )

)

recommendation_prompt = recommendation_prompt_df.first()

recommendation_prompt_template = recommendation_prompt["Prompt_Template"]
recommendation_temperature = recommendation_prompt["Temperature"]
recommendation_max_tokens = recommendation_prompt["Max_Tokens"]

# ============================================================
# Validate Prompt
# ============================================================

if recommendation_prompt_df.count() == 0:

    raise Exception(
        "Active prompt 'Sales Recommendation' not found in AI_Engine_prompt_repository."
    )

# ============================================================
# Read Prompt Details
# ============================================================

prompt_row = recommendation_prompt_df.first()

recommendation_prompt = prompt_row["Prompt_Template"]

temperature = float(prompt_row["Temperature"])

max_tokens = int(prompt_row["Max_Tokens"])

print("Sales Recommendation Prompt Loaded Successfully.")

print(f"Temperature : {temperature}")
print(f"Max Tokens  : {max_tokens}")

# ============================================================
# Prepare Recommendation Context
# ============================================================

print("Preparing Recommendation Context...")

recommendation_context_df = (

    sales_insight_df

    .select(

        col("Sales_SK"),

        col("Sales_Insight")

    )

)

print("Recommendation Context Prepared Successfully.")

# ============================================================
# Validate Output
# ============================================================

validate_table(

    recommendation_context_df,

    "Recommendation Context")


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Cell 18:
# ============================================================
# Generate Business Recommendations
# ============================================================

print("=" * 80)
print("Generate Business Recommendations")
print("=" * 80)

from openai import AzureOpenAI
from datetime import datetime
import builtins

# ------------------------------------------------------------------
# Azure OpenAI Client
# ------------------------------------------------------------------

client = AzureOpenAI(
    api_key=AZURE_OPENAI_API_KEY,
    azure_endpoint=AZURE_OPENAI_ENDPOINT,
    api_version=AZURE_OPENAI_API_VERSION_RESPONSES
)

recommendation_results = []

recommendation_rows = recommendation_context_df.collect()

total_records = len(recommendation_rows)

print(f"Total Records : {total_records}")
print()

# ------------------------------------------------------------------
# Generate Recommendations
# ------------------------------------------------------------------

for index, row in enumerate(recommendation_rows, start=1):

    print(f"Processing {index}/{total_records} - Sales_SK : {row['Sales_SK']}")

    try:

        prompt = recommendation_prompt_template.format(
            sales_insight=row["Sales_Insight"]
        )

        response = client.responses.create(
            model=CHAT_DEPLOYMENT,
            input=[
                {
                    "role": "user",
                    "content": [
                        {
                            "type": "input_text",
                            "text": prompt
                        }
                    ]
                }
            ],
            max_output_tokens=builtins.round(recommendation_max_tokens)
        )

        recommendation = response.output_text

        recommendation_results.append(
            (
                int(row["Sales_SK"]),
                recommendation,
                CHAT_DEPLOYMENT,
                "Sales Recommendation",
                datetime.now()
            )
        )

        print("✓ Completed")

    except Exception as ex:

        print(f"✗ Failed : {ex}")

        recommendation_results.append(
            (
                int(row["Sales_SK"]),
                None,
                CHAT_DEPLOYMENT,
                "Sales Recommendation",
                datetime.now()
            )
        )

print()
print("=" * 80)
print("Creating Recommendation DataFrame")
print("=" * 80)

# ------------------------------------------------------------------
# Create Spark DataFrame
# ------------------------------------------------------------------

recommendation_schema = StructType([
    StructField("Sales_SK", IntegerType(), False),
    StructField("Business_Recommendation", StringType(), True),
    StructField("Model_Name", StringType(), True),
    StructField("Prompt_Name", StringType(), True),
    StructField("Generated_Timestamp", TimestampType(), True)
])

recommendation_df = spark.createDataFrame(
    recommendation_results,
    recommendation_schema
)

validate_table(
    recommendation_df,
    "Business Recommendations"
)

# ------------------------------------------------------------------
# Save Delta Table
# ------------------------------------------------------------------

print()
print("=" * 80)
print("Saving AI Recommendations")
print("=" * 80)

(
    recommendation_df.write
    .format("delta")
    .mode("overwrite")
    .saveAsTable("AI_Engine_recommendations")
)

print("AI_Engine_recommendations created successfully.")

print()

# ------------------------------------------------------------------
# Validate Recommendation DataFrame
# ------------------------------------------------------------------

validate_table(
    spark.table("AI_Engine_recommendations"),
    "AI Engine Recommendations"
)


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
