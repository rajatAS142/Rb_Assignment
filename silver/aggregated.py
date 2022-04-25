# Databricks notebook source
# MAGIC %md
# MAGIC # aggregated Silver Table
# MAGIC 
# MAGIC The goal of this script is to create aggregated Bronze table 
# MAGIC 
# MAGIC The following tables are read:
# MAGIC 
# MAGIC | Table |
# MAGIC | ------ |
# MAGIC | 'rajat.bronze_demographics' |
# MAGIC | 'rajat.bronze_epidemiology' |
# MAGIC | 'rajat.bronze_hospitalizations' |
# MAGIC | 'rajat.bronze_vaccinations' |
# MAGIC | 'rajat.bronze_health' |
# MAGIC | 'rajat.bronze_mobility' |
# MAGIC 
# MAGIC 
# MAGIC The following tables are created:
# MAGIC 
# MAGIC | Tables |
# MAGIC | ------ |
# MAGIC | 'silver_aggregated'|

# COMMAND ----------

import pyspark.sql.functions as F
import datetime as dt
import os
import logging
import io
from re import sub
debug = False
from delta.tables import *

# COMMAND ----------

#General Configuration
silver_datalake_location = "dbfs:/FileStore/RAJAT/BRONZE/aggergated"
silver_database = "rajat"
silver_table_name = "silver_aggregated" 
partition_by = "location_key"

# COMMAND ----------

# MAGIC %md
# MAGIC ## Read Tables

# COMMAND ----------

logging.info(f'reading source table: rajat.bronze_demographics')
demographics_df = spark.table('rajat.bronze_demographics')

logging.info(f'reading source table: rajat.bronze_epidemiology')
epidemiology_df = spark.table('rajat.bronze_epidemiology')

logging.info(f'reading source table: rajat.bronze_hospitalizations')
hospitalizations_df = spark.table('rajat.bronze_hospitalizations')

logging.info(f'reading source table: rajat.bronze_health')
health_df = spark.table('rajat.bronze_health')

logging.info(f'reading source table: rajat.bronze_mobility')
mobility_df = spark.table('rajat.bronze_mobility')

logging.info(f'reading source table: rajat.bronze_vaccinations')
vaccinations_df = spark.table('rajat.bronze_vaccinations')

# COMMAND ----------

# MAGIC %md
# MAGIC ## Applying transformation

# COMMAND ----------

df = epidemiology_df.join(hospitalizations_df, ['location_key', 'date'], "left")\
                    .join(mobility_df, ['location_key', 'date'], "left")\
                    .join(vaccinations_df, ['location_key', 'date'], "left")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Save data to Datalake Silver Layer

# COMMAND ----------

if len(list(filter(lambda x: True if x.name == silver_table_name else False,spark.catalog.listTables(silver_database)))) == 0:
    # TABLE DOES NOT EXIST
    logging.info(f'Creating table and saving data to Silver Layer')
    (
        df
        .write
        .format('delta')
        .mode('overwrite')
        .option('overwriteSchema', 'true')
        .partitionBy(partition_by)
        .option('path', silver_datalake_location)
        .saveAsTable(f'{silver_database}.{silver_table_name}')
    )
    logging.info(f'finished writing data to Silver Layer: {silver_database}.{silver_table_name}')
else:
    # TABLE EXISTS
    logging.info(f'Merging data to Silver Layer')
    table = DeltaTable.forName(spark, f"{silver_database}.{silver_table_name}")
    (
        table.alias("Hot")
        .merge(
            source = save_sales_rep.alias("New"),
            condition = F.expr(f""" Hot.location_key = New.location_key
                                    AND Hot.date = New.date
                                """)
        )
        .whenMatchedUpdate()
        .whenNotMatchedInsertAll()
    ).execute()
    logging.info(f'Finished merging data to Silver Layer')

# COMMAND ----------

dbutils.notebook.exit('....')
