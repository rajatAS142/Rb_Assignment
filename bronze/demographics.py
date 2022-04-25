# Databricks notebook source
# MAGIC %md
# MAGIC # demographics Bronze Table
# MAGIC 
# MAGIC The goal of this script is to create demographics Bronze table 
# MAGIC 
# MAGIC The following tables are read:
# MAGIC 
# MAGIC | Table |
# MAGIC | ------ |
# MAGIC | 'https://storage.googleapis.com/covid19-open-data/v3/demographics.csv' |
# MAGIC 
# MAGIC 
# MAGIC The following tables are created:
# MAGIC 
# MAGIC | Tables |
# MAGIC | ------ |
# MAGIC | 'bronze_demographics'|

# COMMAND ----------

import pyspark.sql.functions as F
import datetime as dt
import os
import logging
import io
from re import sub
debug = False
from delta.tables import *
import pandas

# COMMAND ----------

#General Configuration
bronze_datalake_location = "dbfs:/FileStore/RAJAT/BRONZE/demographics"
checkpoint_path= "dbfs:/FileStore/RAJAT/CONFIG/autoloader_checkpoints/demographics"
raw_table_path = "https://storage.googleapis.com/covid19-open-data/v3/demographics.csv"
table = "bigquery-public-data.covid19_open_data.covid19_open_data"
project_id = "reckitt-training-cloud"
bronze_database = "rajat"
bronze_table_name = "bronze_demographics" 
partition_by = "location_key"

# COMMAND ----------

# MAGIC %md
# MAGIC ## Read Tables

# COMMAND ----------

logging.info(f'reading source tables ...')
data = pandas.read_csv(raw_table_path)
df = spark.createDataFrame(data)

bq_df = spark.read.format("bigquery") \
  .option("table", table) \
  .option("project", project_id) \
  .load()

# COMMAND ----------

demographics_df = bq_df.select(
    "location_key",      
    "population",        
    "population_male",       
    "population_female",     
    "population_rural",      
    "population_urban",      
    "population_largest_city",       
    "population_clustered",      
    "population_density",        
    "human_development_index",       
    "population_age_00_09",      
    "population_age_10_19",      
    "population_age_20_29",      
    "population_age_30_39",      
    "population_age_40_49",      
    "population_age_50_59",      
    "population_age_60_69",      
    "population_age_70_79",      
    "population_age_80_and_older"     
).withColumn("date", F.current_timestamp())

# COMMAND ----------

def replace_where(df, batchId):
    logger.info(f'batch id: {batchId} count: {df.count()}')
    if len(list(filter(lambda x: True if x.name == bronze_table_name else False,spark.catalog.listTables(bronze_database)))) == 0:
        # TABLE DOES NOT EXIST
        logger.info(f'Creating table and saving data to Bronze Layer')
        (df
                .write
                .format('delta')
                .mode('overwrite')
                .option('header', 'true')
                .option('overwriteSchema', 'true')
                .partitionBy(partition_by)
                .option('path', bronze_datalake_location)
                .saveAsTable(f'{bronze_database}.{bronze_table_name}'))
        logger.info(f'finished writing data to Bronze Layer: {bronze_database}.{bronze_table_name}')
    
    else : 
        
        # TABLE EXISTS
        logger.info(f'Merging data to Bronze Layer')
        table = DeltaTable.forName(spark, f"{bronze_database}.{bronze_table_name}")
        (
            table.alias("Hot")
            .merge(
                source = df.alias("New"),
                condition = F.expr(f""" Hot.date = New.date AND 
                                        Hot.location_key = New.location_key """)
            )
            .whenMatchedUpdateAll()
            .whenNotMatchedInsertAll()
        ).execute()
        logging.info(f'Finished merging data to Bronze Layer')

# COMMAND ----------

logging.info(f'writing data to Bronze Layer')
(df
    .write
    .format('delta')
    .mode('overwrite')
    .option('header', 'true')
    .option('overwriteSchema', 'true')
    .partitionBy(partition_by)
    .option('path', bronze_datalake_location)
    .saveAsTable(f'{bronze_database}.{bronze_table_name}'))
logging.info(f'finished writing data to Bronze Layer: {bronze_database}.{bronze_table_name}')

# COMMAND ----------

dbutils.notebook.exit('....')
