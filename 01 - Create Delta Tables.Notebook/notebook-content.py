# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   },
# META   "dependencies": {
# META     "lakehouse": {
# META       "default_lakehouse": "2c52a91e-6ef4-4364-a525-33ceae21618a",
# META       "default_lakehouse_name": "wwilakehouse",
# META       "default_lakehouse_workspace_id": "953110e1-d237-4866-9e9a-f4278a2eefcd",
# META       "known_lakehouses": [
# META         {
# META           "id": "82e78e41-11a3-448e-a1aa-0bc48fc09cb6"
# META         },
# META         {
# META           "id": "2c52a91e-6ef4-4364-a525-33ceae21618a"
# META         }
# META       ]
# META     }
# META   }
# META }

# MARKDOWN ********************

# ### Spark session configuration
# This cell sets Spark session settings to enable _Verti-Parquet_ and _Optimize on Write_. More details about _Verti-Parquet_ and _Optimize on Write_ in tutorial document.

# CELL ********************

# Copyright (c) Microsoft Corporation.
# Licensed under the MIT License.
#https://learn.microsoft.com/en-us/fabric/data-engineering/tutorial-lakehouse-data-preparation

spark.conf.set("spark.sql.parquet.vorder.enabled", "true")
spark.conf.set("spark.microsoft.delta.optimizeWrite.enabled", "true")
spark.conf.set("spark.microsoft.delta.optimizeWrite.binSize", "1073741824")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ### Fact - Sale
# 
# This cell reads raw data from the _Files_ section of the lakehouse, adds additional columns for different date parts and the same information is being used to create partitioned fact delta table.

# CELL ********************

from pyspark.sql.functions import col, year, month, quarter

table_name = 'fact_sale'

df = spark.read.format("parquet").load('Files/wwi-raw-data/WideWorldImportersDW/parquet/full/fact_sale_1y_full')
df = df.withColumn('Year', year(col("InvoiceDateKey")))
df = df.withColumn('Quarter', quarter(col("InvoiceDateKey")))
df = df.withColumn('Month', month(col("InvoiceDateKey")))

df.write.mode("overwrite").format("delta").partitionBy("Year","Quarter").save("Tables/" + table_name)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ### Dimensions
# This cell creates a function to read raw data from the _Files_ section of the lakehouse for the table name passed as a parameter. Next, it creates a list of dimension tables. Finally, it has a _for loop_ to loop through the list of tables and call above function with each table name as parameter to read data for that specific table and create delta table.

# CELL ********************

from pyspark.sql.types import *

def loadFullDataFromSource(table_name):
    df = spark.read.format("parquet").load('Files/wwi-raw-data/WideWorldImportersDW/parquet/full/' + table_name)
    df = df.select([c for c in df.columns if c != 'Photo'])
    df.write.mode("overwrite").format("delta").save("Tables/" + table_name)

full_tables = [
    'dimension_city',
    'dimension_customer',
    'dimension_date',
    'dimension_employee',
    'dimension_stock_item'
    ]

for table in full_tables:
    loadFullDataFromSource(table)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
