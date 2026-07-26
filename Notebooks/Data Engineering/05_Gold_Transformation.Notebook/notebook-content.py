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

# # 05 - Gold Transformation
# 
# ## Objective
# 
# Transform curated Silver Delta tables into enterprise-ready Business Model tables using dimensional modelling (Star Schema).
# 
# This notebook performs:
# 
# - Load Silver Delta tables
# - Build Business Dimensions
# - Generate Surrogate Keys (SK)
# - Build Fact Tables
# - Apply business transformations
# - Validate dimensional relationships
# - Create Gold Delta tables
# - Generate execution summary
# 
# ---
# 
# ## Business Models
# 
# ### Dimension Tables
# 
# - Business_Models_dim_calendar
# - Business_Models_dim_customer
# - Business_Models_dim_product
# - Business_Models_dim_seller
# - Business_Models_dim_geography
# 
# ### Fact Tables
# 
# - Business_Models_fact_sales
# 
# ---
# 
# ### Future Layers
# 
# The Business Model layer serves as the foundation for:
# 
# - Machine Learning Feature Engineering
# - AI Engineering
# - Power BI Semantic Models
# 
# ---
# 
# ## Layer
# 
# Silver ➜ Gold
# 
# ---
# 
# ## Architecture
# 
# Silver Layer  
# ↓  
# Business Dimensions  
# ↓  
# Fact Tables  
# ↓  
# Power BI / Machine Learning / AI Engineering
# 
# ---
# 
# ## Design Principles
# 
# - Enterprise-ready dimensional modelling
# - Star Schema architecture
# - Surrogate Keys (SK)
# - Self-documenting object names
# - Business-first design
# - Reusable and scalable data models


# CELL ********************

# Cell 1:
# =====================================================================================
# IMPORTS
# =====================================================================================

from pyspark.sql import functions as F
from pyspark.sql.window import Window
from pyspark.sql.functions import *

import time
from datetime import datetime

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Cell 2:
# =====================================================================================
# CONFIGURATION
# =====================================================================================

NOTEBOOK_NAME = "05_Gold_Transformation"
LAYER = "Gold"

START_TIME = time.time()

print("=" * 80)
print("PROJECT TITAN")
print(f"Notebook : {NOTEBOOK_NAME}")
print(f"Layer    : {LAYER}")
print(f"Started  : {datetime.now()}")
print("=" * 80)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Cell 3:
# =====================================================================================
# Helper Functions
# =====================================================================================

def validate_dimension(df, table_name):
    """
    Validate and preview a Business Model Dimension.
    """

    print("=" * 80)
    print(f"{table_name} Summary")
    print("=" * 80)

    print(f"Total Records : {df.count()}")
    print(f"Total Columns : {len(df.columns)}")

    display(df.limit(10))

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Cell 4:
# =====================================================================================
# Read Silver Tables
# =====================================================================================

print("Loading Silver Layer tables...")

silver_tables = {
    "customers": spark.read.table("silver_customers"),
    "orders": spark.read.table("silver_orders"),
    "order_items": spark.read.table("silver_order_items"),
    "products": spark.read.table("silver_products"),
    "payments": spark.read.table("silver_payments"),
    "reviews": spark.read.table("silver_reviews"),
    "sellers": spark.read.table("silver_sellers"),
    "geolocation": spark.read.table("silver_geolocation")
}

silver_customers = silver_tables["customers"]
silver_orders = silver_tables["orders"]
silver_order_items = silver_tables["order_items"]
silver_products = silver_tables["products"]
silver_payments = silver_tables["payments"]
silver_reviews = silver_tables["reviews"]
silver_sellers = silver_tables["sellers"]
silver_geolocation = silver_tables["geolocation"]

print(f"✅ Loaded {len(silver_tables)} Silver Layer tables successfully.")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Cell 5:
# =====================================================================================
# SOURCE DATA OVERVIEW
# =====================================================================================

datasets = {
    "Customers": silver_customers,
    "Orders": silver_orders,
    "Order Items": silver_order_items,
    "Products": silver_products,
    "Sellers": silver_sellers,
    "Payments": silver_payments,
    "Reviews": silver_reviews
}

for name, df in datasets.items():
    print(f"{name:<20} Rows : {df.count():>8} | Columns : {len(df.columns)}")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# # Business Model Design
# 
# ## Star Schema
# 
# The Gold layer follows a dimensional modelling approach (Star Schema) to provide
# high-performance analytical datasets for reporting, Machine Learning and AI Engineering.
# 
# ### Dimension Tables
# 
# - Business_Models_dim_calendar
# - Business_Models_dim_customer
# - Business_Models_dim_product
# - Business_Models_dim_seller
# - Business_Models_dim_geography
# 
# ### Fact Tables
# 
# - Business_Models_fact_sales
# 
# ---
# 
# ## Relationships
# 
# Business_Models_fact_sales is the central fact table and references all business dimensions through Surrogate Keys (SK).
# 
# Business_Models_dim_calendar  ───────┐
# Business_Models_dim_customer  ───────┤
# Business_Models_dim_product   ───────┤
# Business_Models_dim_seller    ───────┤
# Business_Models_dim_geography ───────┘
#                   │
#                   ▼
#         Business_Models_fact_sales

# MARKDOWN ********************

# # Build Business_Models_dim_calendar
# 
# ## Objective
# 
# Create a reusable enterprise Calendar Dimension using the order dates from the Silver layer.
# 
# This dimension provides standardized calendar attributes for reporting, analytics, Machine Learning and AI Engineering.
# 
# ---
# 
# ## Source
# 
# - silver_orders
# 
# ---
# 
# ## Output
# 
# - Business_Models_dim_calendar
# 
# ---
# 
# ## Business Rules
# 
# - Generate one record per calendar date
# - Create deterministic Calendar_SK (YYYYMMDD)
# - Derive standard calendar attributes
# - Generate audit columns
# - Support Star Schema joins

# CELL ********************

# Cell 6:
# =====================================================================================
# Build Business_Models_dim_calendar
# =====================================================================================

from pyspark.sql.functions import *
from pyspark.sql.types import DateType
from pyspark.sql.window import Window

print("Building Business_Models_dim_calendar...")

# -----------------------------------------------------------------------------
# Get Calendar Range from Orders
# -----------------------------------------------------------------------------

date_range = (
    silver_orders
    .select(
        min(col("order_purchase_timestamp")).alias("min_date"),
        max(col("order_purchase_timestamp")).alias("max_date")
    )
    .collect()[0]
)

min_date = date_range["min_date"].date()
max_date = date_range["max_date"].date()

print(f"Calendar Start Date : {min_date}")
print(f"Calendar End Date   : {max_date}")

# -----------------------------------------------------------------------------
# Generate Calendar
# -----------------------------------------------------------------------------

calendar = spark.sql(f"""
SELECT explode(
    sequence(
        to_date('{min_date}'),
        to_date('{max_date}'),
        interval 1 day
    )
) AS Calendar_Date
""")

# -----------------------------------------------------------------------------
# Build Calendar Dimension
# -----------------------------------------------------------------------------

Business_Models_dim_calendar = (

    calendar

    .withColumn(
        "Calendar_SK",
        date_format("Calendar_Date", "yyyyMMdd").cast("int")
    )

    .withColumn(
        "Day_Of_Month",
        dayofmonth("Calendar_Date")
    )

    .withColumn(
        "Day_Name",
        date_format("Calendar_Date", "EEEE")
    )

    .withColumn(
        "Day_Of_Week",
        dayofweek("Calendar_Date")
    )

    .withColumn(
        "Week_Number",
        weekofyear("Calendar_Date")
    )

    .withColumn(
        "Month",
        month("Calendar_Date")
    )

    .withColumn(
        "Month_Name",
        date_format("Calendar_Date", "MMMM")
    )

    .withColumn(
        "Quarter",
        concat(lit("Q"), quarter("Calendar_Date"))
    )

    .withColumn(
        "Year",
        year("Calendar_Date")
    )

    .withColumn(
        "Month_Year",
        date_format("Calendar_Date", "MMM-yyyy")
    )

    .withColumn(
        "Quarter_Year",
        concat(
            lit("Q"),
            quarter("Calendar_Date"),
            lit("-"),
            year("Calendar_Date")
        )
    )

    .withColumn(
        "Is_Weekend",
        when(dayofweek("Calendar_Date").isin(1,7),"Yes")
        .otherwise("No")
    )

    .withColumn(
        "Day_Type",
        when(dayofweek("Calendar_Date").isin(1,7), "Weekend")
        .otherwise("Weekday")
    )

    .withColumn(
        "Created_Timestamp",
        current_timestamp()
    )

    .withColumn(
        "Modified_Timestamp",
        current_timestamp()
    )

    .select(
        "Calendar_SK",
        "Calendar_Date",
        "Year",
        "Quarter",
        "Quarter_Year",
        "Month",
        "Month_Name",
        "Month_Year",
        "Week_Number",
        "Day_Of_Week",
        "Day_Name",
        "Day_Of_Month",
        "Is_Weekend",
        "Day_Type",
        "Created_Timestamp",
        "Modified_Timestamp"
    )

    .orderBy("Calendar_Date")

)

print("Business_Models_dim_calendar created successfully.")

# -----------------------------------------------------------------------------
# Validate Business_Models_dim_calendar
# -----------------------------------------------------------------------------

validate_dimension(
    Business_Models_dim_calendar,
    "Business_Models_dim_calendar"
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# # Build Business_Models_dim_customer
# 
# ## Objective
# 
# Create the Customer Dimension by transforming customer master data from the Silver layer into a reusable business dimension.
# 
# ---
# 
# ## Source
# 
# - silver_customers
# 
# ---
# 
# ## Output
# 
# - Business_Models_dim_customer

# CELL ********************

# Cell 7:
# =====================================================================================
# Build Business_Models_dim_customer
# =====================================================================================

print("Building Business_Models_dim_customer...")

from pyspark.sql.functions import *
from pyspark.sql.window import Window

# -----------------------------------------------------------------------------
# Create Surrogate Key Window
# -----------------------------------------------------------------------------

customer_window = Window.orderBy("Customer_ID")

# -----------------------------------------------------------------------------
# Build Customer Dimension
# -----------------------------------------------------------------------------

Business_Models_dim_customer = (

    silver_customers

    # Select and rename business columns
    .select(
        col("customer_id").alias("Customer_ID"),
        col("customer_unique_id").alias("Customer_Unique_ID"),
        col("customer_zip_code_prefix").alias("Zip_Code_Prefix"),
        initcap(col("customer_city")).alias("Customer_City"),
        upper(col("customer_state")).alias("Customer_State")
    )

    # Generate Surrogate Key
    .withColumn(
        "Customer_SK",
        row_number().over(customer_window)
    )

    # Audit Columns
    .withColumn(
        "Created_Timestamp",
        current_timestamp()
    )

    .withColumn(
        "Modified_Timestamp",
        current_timestamp()
    )

    # Final Column Order
    .select(
        "Customer_SK",
        "Customer_ID",
        "Customer_Unique_ID",
        "Zip_Code_Prefix",
        "Customer_City",
        "Customer_State",
        "Created_Timestamp",
        "Modified_Timestamp"
    )

    .orderBy("Customer_SK")

)

print("Business_Models_dim_customer created successfully.")

# -----------------------------------------------------------------------------
# Validate Business_Models_dim_customer
# -----------------------------------------------------------------------------

validate_dimension(
    Business_Models_dim_customer,
    "Business_Models_dim_customer"
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# # Build Business_Models_dim_product
# 
# ## Objective
# 
# Create the Product Dimension by transforming product master data from the Silver layer into a reusable business dimension.
# 
# ---
# 
# ## Source
# 
# - silver_products
# 
# ---
# 
# ## Output
# 
# - Business_Models_dim_product
# 
# ---
# 
# ## Business Rules
# 
# - Generate a Surrogate Key (Product_SK)
# - Standardize business-friendly column names
# - Calculate Product Volume (CM³)
# - Generate audit columns
# - Preserve source product category values

# CELL ********************

# Cell 8:
# =====================================================================================
# Build Business_Models_dim_product
# =====================================================================================

print("Building Business_Models_dim_product...")

from pyspark.sql.functions import *
from pyspark.sql.window import Window

# -----------------------------------------------------------------------------
# Create Surrogate Key Window
# -----------------------------------------------------------------------------

product_window = Window.orderBy("Product_ID")

# -----------------------------------------------------------------------------
# Build Product Dimension
# -----------------------------------------------------------------------------

Business_Models_dim_product = (

    silver_products

    # Select and Rename Business Columns
    .select(
        col("product_id").alias("Product_ID"),
        col("product_category_name").alias("Product_Category"),
        col("product_name_length").alias("Product_Name_Length"),
        col("product_description_length").alias("Product_Description_Length"),
        col("product_photos_qty").alias("Product_Photos_Count"),
        col("product_weight_g").alias("Product_Weight_Grams"),
        col("product_length_cm").alias("Product_Length_CM"),
        col("product_height_cm").alias("Product_Height_CM"),
        col("product_width_cm").alias("Product_Width_CM")
    )

    # Generate Surrogate Key
    .withColumn(
        "Product_SK",
        row_number().over(product_window)
    )

    # Derived Business Attribute
    .withColumn(
        "Product_Volume_CM3",
        when(
            col("Product_Length_CM").isNull() |
            col("Product_Width_CM").isNull() |
            col("Product_Height_CM").isNull(),
            None
        ).otherwise(
            col("Product_Length_CM") *
            col("Product_Width_CM") *
            col("Product_Height_CM")
        )
    )

    # Audit Columns
    .withColumn(
        "Created_Timestamp",
        current_timestamp()
    )

    .withColumn(
        "Modified_Timestamp",
        current_timestamp()
    )

    # Final Column Order
    .select(
        "Product_SK",
        "Product_ID",
        "Product_Category",
        "Product_Name_Length",
        "Product_Description_Length",
        "Product_Photos_Count",
        "Product_Weight_Grams",
        "Product_Length_CM",
        "Product_Height_CM",
        "Product_Width_CM",
        "Product_Volume_CM3",
        "Created_Timestamp",
        "Modified_Timestamp"
    )

    .orderBy("Product_SK")

)

print("Business_Models_dim_product created successfully.")

# -----------------------------------------------------------------------------
# Validate Business_Models_dim_product
# -----------------------------------------------------------------------------

validate_dimension(
    Business_Models_dim_product,
    "Business_Models_dim_product"
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# # Build Business_Models_dim_seller
# 
# ## Objective
# 
# Create the Seller Dimension by transforming seller master data from the Silver layer into a reusable business dimension.
# 
# ---
# 
# ## Source
# 
# - silver_sellers
# 
# ---
# 
# ## Output
# 
# - Business_Models_dim_seller
# 
# ---
# 
# ## Business Rules
# 
# - Generate a Surrogate Key (Seller_SK)
# - Standardize business-friendly column names
# - Standardize Seller City and State values
# - Generate audit columns

# CELL ********************

# Cell 9:
# =====================================================================================
# Build Business_Models_dim_seller
# =====================================================================================

print("Building Business_Models_dim_seller...")

from pyspark.sql.functions import *
from pyspark.sql.window import Window

# -----------------------------------------------------------------------------
# Create Surrogate Key Window
# -----------------------------------------------------------------------------

seller_window = Window.orderBy("Seller_ID")

# -----------------------------------------------------------------------------
# Build Seller Dimension
# -----------------------------------------------------------------------------

Business_Models_dim_seller = (

    silver_sellers

    # Select and Rename Business Columns
    .select(
        col("seller_id").alias("Seller_ID"),
        col("seller_zip_code_prefix").alias("Zip_Code_Prefix"),
        col("seller_city").alias("Seller_City"),
        col("seller_state").alias("Seller_State")
    )

    # Standardize Business Values
    .withColumn(
        "Seller_City",
        initcap(col("Seller_City"))
    )

    .withColumn(
        "Seller_State",
        upper(col("Seller_State"))
    )

    # Generate Surrogate Key
    .withColumn(
        "Seller_SK",
        row_number().over(seller_window)
    )

    # Audit Columns
    .withColumn(
        "Created_Timestamp",
        current_timestamp()
    )

    .withColumn(
        "Modified_Timestamp",
        current_timestamp()
    )

    # Final Column Order
    .select(
        "Seller_SK",
        "Seller_ID",
        "Zip_Code_Prefix",
        "Seller_City",
        "Seller_State",
        "Created_Timestamp",
        "Modified_Timestamp"
    )

    .orderBy("Seller_SK")

)

print("Business_Models_dim_seller created successfully.")

# -----------------------------------------------------------------------------
# Validate Business Model
# -----------------------------------------------------------------------------

validate_dimension(
    Business_Models_dim_seller,
    "Business_Models_dim_seller"
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# # Build Business_Models_dim_geography
# 
# ## Objective
# 
# Create the Geography Dimension by transforming geolocation master data from the Silver layer into a reusable business dimension.
# 
# ---
# 
# ## Source
# 
# - silver_geolocation
# 
# ---
# 
# ## Output
# 
# - Business_Models_dim_geography
# 
# ---
# 
# ## Business Rules
# 
# - Generate a Surrogate Key (Geography_SK)
# - Remove duplicate geography records
# - Standardize City and State values
# - Preserve Latitude and Longitude
# - Generate audit columns

# CELL ********************

# Cell 10:
# =====================================================================================
# Build Business_Models_dim_geography
# =====================================================================================

print("Building Business_Models_dim_geography...")

from pyspark.sql.functions import *
from pyspark.sql.window import Window

# -----------------------------------------------------------------------------
# Create Surrogate Key Window
# -----------------------------------------------------------------------------

geography_window = Window.orderBy("Zip_Code_Prefix")

# -----------------------------------------------------------------------------
# Build Geography Dimension
# -----------------------------------------------------------------------------

Business_Models_dim_geography = (

    silver_geolocation

    # Select and Rename Business Columns
    .select(
        col("geolocation_zip_code_prefix").alias("Zip_Code_Prefix"),
        initcap(col("geolocation_city")).alias("City"),
        upper(col("geolocation_state")).alias("State"),
        col("geolocation_lat").alias("Latitude"),
        col("geolocation_lng").alias("Longitude")
    )

    # Create One Geography Record per ZIP Code
    .groupBy("Zip_Code_Prefix")

    .agg(
        first("City").alias("City"),
        first("State").alias("State"),
        first("Latitude").alias("Latitude"),
        first("Longitude").alias("Longitude")
    )

    # Generate Surrogate Key
    .withColumn(
        "Geography_SK",
        row_number().over(geography_window)
    )

    # Audit Columns
    .withColumn(
        "Created_Timestamp",
        current_timestamp()
    )

    .withColumn(
        "Modified_Timestamp",
        current_timestamp()
    )

    # Final Column Order
    .select(
        "Geography_SK",
        "Zip_Code_Prefix",
        "City",
        "State",
        "Latitude",
        "Longitude",
        "Created_Timestamp",
        "Modified_Timestamp"
    )

    .orderBy("Geography_SK")

)

print("Business_Models_dim_geography created successfully.")

# -----------------------------------------------------------------------------
# Validate Business Model
# -----------------------------------------------------------------------------

validate_dimension(
    Business_Models_dim_geography,
    "Business_Models_dim_geography"
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# # Build Business_Models_fact_sales
# 
# ## Objective
# 
# Create the Sales Fact table by integrating orders, order items, payment information, review information, and business dimensions into a centralized Sales Fact table.
# 
# ---
# 
# ## Source Tables
# 
# - silver_orders
# - silver_order_items
# - silver_payments
# - silver_reviews
# - Business_Models_dim_customer
# - Business_Models_dim_product
# - Business_Models_dim_seller
# - Business_Models_dim_geography
# - Business_Models_dim_calendar
# 
# ---
# 
# ## Output Table
# 
# Business_Models_fact_sales
# 
# ---
# 
# ## Business Rules
# 
# - One record represents one Order Item.
# - Aggregate Payment Information at Order level.
# - Aggregate Review Information at Order level.
# - Lookup Surrogate Keys from Business Dimensions.
# - Preserve Business Measures.
# - Generate Audit Columns.

# CELL ********************

# Cell 11:
# =============================================================================
# Build Business_Models_fact_sales
# =============================================================================

print("Building Business_Models_fact_sales...")

# -----------------------------------------------------------------------------
# Step 1 : Aggregate Payment Information
# -----------------------------------------------------------------------------

payment_summary_df = (
    silver_payments
        .groupBy("order_id")
        .agg(
            concat_ws(", ", collect_set("payment_type")).alias("Payment_Type"),
            max("payment_installments").alias("Payment_Installments"),
            sum("payment_value").alias("Payment_Value")
        )
)

# -----------------------------------------------------------------------------
# Step 2 : Aggregate Review Information
# -----------------------------------------------------------------------------

review_summary_df = (
    silver_reviews
        .groupBy("order_id")
        .agg(
            first("review_score").alias("Review_Score")
        )
)

# -----------------------------------------------------------------------------
# Step 3 : Build Base Sales Dataset
# -----------------------------------------------------------------------------

base_sales_df = (
    silver_order_items.alias("oi")

    .join(
        silver_orders.alias("o"),
        "order_id",
        "inner"
    )

    .join(
        payment_summary_df.alias("p"),
        "order_id",
        "left"
    )

    .join(
        review_summary_df.alias("r"),
        "order_id",
        "left"
    )

    .select(

        # Business Keys
        col("order_id").alias("Order_ID"),
        col("order_item_id").alias("Order_Item_ID"),

        col("customer_id"),
        col("product_id"),
        col("seller_id"),

        # Order Information
        col("order_status").alias("Order_Status"),

        # Order Dates
        col("order_purchase_timestamp").alias("Order_Purchase_Timestamp"),
        col("order_approved_at").alias("Order_Approved_Timestamp"),
        col("order_delivered_carrier_date").alias("Order_Delivered_Carrier_Timestamp"),
        col("order_delivered_customer_date").alias("Order_Delivered_Customer_Timestamp"),
        col("order_estimated_delivery_date").alias("Order_Estimated_Delivery_Timestamp"),

        # Delivery Metrics
        col("delivery_days").alias("Delivery_Days"),
        col("delivery_delay_days").alias("Delivery_Delay_Days"),
        col("delivery_status").alias("Delivery_Status"),
        col("delivery_performance").alias("Delivery_Performance"),

        # Financial Measures
        col("price").alias("Price"),
        col("freight_value").alias("Freight_Value"),
        col("total_item_value").alias("Total_Item_Value"),
        col("is_free_shipping").alias("Is_Free_Shipping"),

        # Payment
        col("Payment_Type"),
        col("Payment_Installments"),
        col("Payment_Value"),

        # Reviews
        col("Review_Score")
    )
)

# -----------------------------------------------------------------------------
# Step 4 : Lookup Customer Dimension
# -----------------------------------------------------------------------------

sales_with_customer_df = (

    base_sales_df.alias("f")

    .join(

        Business_Models_dim_customer.alias("c"),

        col("f.customer_id") == col("c.Customer_ID"),

        "left"

    )

    .select(

        col("c.Customer_SK"),

        col("c.Zip_Code_Prefix").alias("Customer_Zip_Code_Prefix"),

        col("f.*")

    )

)

# -----------------------------------------------------------------------------
# Step 5 : Lookup Product Dimension
# -----------------------------------------------------------------------------

sales_with_product_df = (

    sales_with_customer_df.alias("f")

    .join(

        Business_Models_dim_product.alias("p"),

        col("f.product_id") == col("p.Product_ID"),

        "left"

    )

    .select(

        col("f.*"),

        col("p.Product_SK")

    )

)

print("Completed through Product Dimension lookup.")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Cell 12:
# -----------------------------------------------------------------------------
# Step 6 : Lookup Seller Dimension
# -----------------------------------------------------------------------------

sales_with_seller_df = (

    sales_with_product_df.alias("f")

    .join(

        Business_Models_dim_seller.alias("s"),

        col("f.seller_id") == col("s.Seller_ID"),

        "left"

    )

    .select(

        col("f.*"),

        col("s.Seller_SK")

    )

)

# -----------------------------------------------------------------------------
# Step 7 : Lookup Geography Dimension
# -----------------------------------------------------------------------------

sales_with_geography_df = (

    sales_with_seller_df.alias("f")

    .join(

        Business_Models_dim_geography.alias("g"),

        col("f.Customer_Zip_Code_Prefix") == col("g.Zip_Code_Prefix"),

        "left"

    )

    .select(

        col("f.*"),

        col("g.Geography_SK")

    )

)

# -----------------------------------------------------------------------------
# Step 8 : Lookup Calendar Dimension
# -----------------------------------------------------------------------------

sales_with_calendar_df = (

    sales_with_geography_df.alias("f")

    .join(

        Business_Models_dim_calendar.alias("c"),

        to_date(col("f.Order_Purchase_Timestamp")) == col("c.Calendar_Date"),

        "left"

    )

    .select(

        col("f.*"),

        col("c.Calendar_SK")

    )

)

# -----------------------------------------------------------------------------
# Step 9 : Generate Sales Surrogate Key
# -----------------------------------------------------------------------------

sales_with_key_df = (

    sales_with_calendar_df

    .withColumn(

        "Sales_SK",

        row_number().over(

            Window.orderBy(

                "Order_ID",
                "Order_Item_ID"

            )

        )

    )

)

# -----------------------------------------------------------------------------
# Step 10 : Add Audit Columns
# -----------------------------------------------------------------------------

sales_with_audit_df = (

    sales_with_key_df

    .withColumn(

        "Created_Timestamp",

        current_timestamp()

    )

    .withColumn(

        "Modified_Timestamp",

        current_timestamp()

    )

)

# -----------------------------------------------------------------------------
# Step 11 : Final Column Selection
# -----------------------------------------------------------------------------

Business_Models_fact_sales = (

    sales_with_audit_df.select(

        # Surrogate Key
        "Sales_SK",

        # Business Keys
        "Order_ID",
        "Order_Item_ID",

        # Dimension Keys
        "Customer_SK",
        "Product_SK",
        "Seller_SK",
        "Geography_SK",
        "Calendar_SK",

        # Order Details
        "Order_Status",

        # Dates
        "Order_Purchase_Timestamp",
        "Order_Approved_Timestamp",
        "Order_Delivered_Carrier_Timestamp",
        "Order_Delivered_Customer_Timestamp",
        "Order_Estimated_Delivery_Timestamp",

        # Delivery Metrics
        "Delivery_Days",
        "Delivery_Delay_Days",
        "Delivery_Status",
        "Delivery_Performance",

        # Financial Measures
        "Price",
        "Freight_Value",
        "Total_Item_Value",
        "Is_Free_Shipping",

        # Payment
        "Payment_Type",
        "Payment_Installments",
        "Payment_Value",

        # Review
        "Review_Score",

        # Audit
        "Created_Timestamp",
        "Modified_Timestamp"

    )

)

print("Business_Models_fact_sales created successfully.")

# -----------------------------------------------------------------------------
# Validate Business Model
# -----------------------------------------------------------------------------

validate_dimension(

    Business_Models_fact_sales,

    "Business_Models_fact_sales"

)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Cell 13:
# =============================================================================
# STEP 12: Write Gold Business Models
# =============================================================================

print("=" * 80)
print("Writing Gold Business Models to Lakehouse...")
print("=" * 80)

# Calendar Dimension
Business_Models_dim_calendar.write \
    .mode("overwrite") \
    .format("delta") \
    .saveAsTable("Business_Models_dim_calendar")

print("✓ Business_Models_dim_calendar written.")

# Customer Dimension
Business_Models_dim_customer.write \
    .mode("overwrite") \
    .format("delta") \
    .saveAsTable("Business_Models_dim_customer")

print("✓ Business_Models_dim_customer written.")

# Product Dimension
Business_Models_dim_product.write \
    .mode("overwrite") \
    .format("delta") \
    .saveAsTable("Business_Models_dim_product")

print("✓ Business_Models_dim_product written.")

# Seller Dimension
Business_Models_dim_seller.write \
    .mode("overwrite") \
    .format("delta") \
    .saveAsTable("Business_Models_dim_seller")

print("✓ Business_Models_dim_seller written.")

# Geography Dimension
Business_Models_dim_geography.write \
    .mode("overwrite") \
    .format("delta") \
    .saveAsTable("Business_Models_dim_geography")

print("✓ Business_Models_dim_geography written.")

# Sales Fact
Business_Models_fact_sales.write \
    .mode("overwrite") \
    .format("delta") \
    .saveAsTable("Business_Models_fact_sales")

print("✓ Business_Models_fact_sales written.")

print("=" * 80)
print("All Gold Business Models successfully written to the Lakehouse.")
print("=" * 80)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Cell 14:
# =============================================================================
# STEP 13: Validate Persisted Gold Tables
# =============================================================================

gold_tables = [
    "Business_Models_dim_calendar",
    "Business_Models_dim_customer",
    "Business_Models_dim_product",
    "Business_Models_dim_seller",
    "Business_Models_dim_geography",
    "Business_Models_fact_sales"
]

for table in gold_tables:
    df = spark.table(table)

    print("=" * 80)
    print(table)
    print(f"Rows    : {df.count()}")
    print(f"Columns : {len(df.columns)}")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
