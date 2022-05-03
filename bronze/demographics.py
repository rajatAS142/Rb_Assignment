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
import pyspark.sql.types as T
import datetime as dt
import os
import logging
import io
from re import sub
debug = False
from delta.tables import *
import pandas
import smtplib

# COMMAND ----------

#General Configuration
script_execution_start_time = dt.datetime.now()
app_name = 'covid-19'
bronze_datalake_location = "dbfs:/FileStore/RAJAT/BRONZE/demographics"
checkpoint_path= "dbfs:/FileStore/RAJAT/CONFIG/autoloader_checkpoints/demographics"
raw_table_path = "https://storage.googleapis.com/covid19-open-data/v3/demographics.csv"
bronze_database = "rajat"
bronze_table_name = "bronze_demographics" 
partition_by = "date"
my_path = '/FileStore/RAJAT/BRONZE/demographics'
notebook_name = dbutils.notebook.entry_point.getDbutils().notebook().getContext().notebookPath().get()
logger = logging.getLogger(app_name)
#email properties
gmail_user = '*******'
gmail_password = '********'
sent_from = gmail_user
to = ['rajatk@sigmoidanalytics.com']
subject = 'Alert for job fail'
email_text = f'Hi Rajat,\n\t{bronze_table_name} job in databricks failed to execute.\nThanks'

# COMMAND ----------

# MAGIC %sql
# MAGIC create database if not exists rajat;
# MAGIC 
# MAGIC USE 'rajat';
# MAGIC 
# MAGIC CREATE TABLE IF NOT EXISTS `demographics` (
# MAGIC   `location_key` varchar(10) NOT NULL default 'NA',       
# MAGIC   `population` double NULL,     
# MAGIC   `population_male`  double NULL,     
# MAGIC   `population_female` double  NULL,    
# MAGIC   `population_rural`  double NULL,
# MAGIC   `population_urban` double NULL,    
# MAGIC   `population_largest_city` double NULL,
# MAGIC   `population_clustered` double NULL,
# MAGIC   `population_density` double NULL,
# MAGIC   `human_development_index` double NULL,    
# MAGIC   `population_age_00_09` double NULL,
# MAGIC   `population_age_10_19` double NULL,
# MAGIC   `population_age_20_29` double NULL,
# MAGIC   `population_age_30_39` double NULL,    
# MAGIC   `population_age_40_49` double NULL,
# MAGIC   `population_age_50_59` double NULL,
# MAGIC   `population_age_60_69` double NULL,
# MAGIC   `population_age_70_79` double NULL,
# MAGIC   `population_age_80_and_older` double NULL,
# MAGIC );

# COMMAND ----------

# MAGIC %md
# MAGIC ## Read from source

# COMMAND ----------

logger.info(f'reading source tables ...')
data = pandas.read_csv(raw_table_path)
df = spark.createDataFrame(data)
logger.info(f'row count : {df.count()}')

# COMMAND ----------

# MAGIC %md
# MAGIC ##Full load and incremental load

# COMMAND ----------

try:
    cf = spark.table('rajat.demographics')
    if cf.count() == 0:
        # TABLE IS EMPTY
        logging.info(f'Creating table and saving data to Bronze Layer')
        (df
                .write
                .format('delta')
                .mode('overwrite')
                .option('header', 'true')
                .option('overwriteSchema', 'true')
                .partitionBy(partition_by)
                .saveAsTable(f'{bronze_database}.{bronze_table_name}'))
        logger.info(f'finished writing data to Bronze Layer: {bronze_database}.{bronze_table_name}')

    else : 

        # TABLE EXISTS
        logging.info(f'Merging data to Bronze Layer')
        table = DeltaTable.forName(spark, f"{bronze_database}.{bronze_table_name}")
        (
            table.alias("Hot")
            .merge(
                source = df.alias("New"),
                condition = F.expr(f""" Hot.population = New.population AND 
                                        Hot.location_key = New.location_key """)
            )
            .whenMatchedUpdate(set =
            {
              "location_key" : "source.location_key",
              "population": "source.population",
              "population_male": "source.population_male",
              "population_female": "source.population_female",
              "population_rural": "source.population_rural",
              "population_urban": "source.population_urban",
              "population_largest_city" : "source.population_largest_city",
              "population_clustered" : "source.population_clustered",
              "population_density" : "source.population_density",
              "human_development_index" : "source.human_development_index",
              "population_age_00_09" : "source.population_age_00_09",
              "population_age_10_19" : "source.population_age_10_19",
              "population_age_20_29" : "source.population_age_20_29",
              "population_age_30_39" : "source.population_age_30_39",
              "population_age_40_49" : "source.population_age_40_49",
              "population_age_50_59" : "source.population_age_50_59",
              "population_age_60_69" : "source.population_age_60_69",
              "population_age_70_79" : "source.population_age_70_79",
              "population_age_80_and_older" : "source.population_age_80_and_older"

            })
            .whenNotMatchedInsertAll()
        ).execute()
        logging.info(f'Finished merging data to Bronze Layer')
    
except Exception as e:
    try:
        server = smtplib.SMTP_SSL('smtp.gmail.com', 465)
        server.ehlo()
        server.login(gmail_user, gmail_password)
        server.sendmail(sent_from, to, email_text)
        server.close()

        print ('Email sent!')
    except Exception as e:
        print(e)
        print ('Mail not sent...')    
    

# COMMAND ----------

script_execution_end_time = dt.datetime.now()

final_output = {
                'notebook_name': notebook_name, 
                'start_timestamp': script_execution_start_time.strftime('%Y-%m-%d %H:%m:%S'), 
                'end_timestamp': script_execution_end_time.strftime('%Y-%m-%d %H:%m:%S'),
                'duration': str(script_execution_end_time - script_execution_start_time),
               }

# COMMAND ----------

dbutils.notebook.exit(final_output)
