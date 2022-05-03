# Databricks notebook source
# MAGIC %md
# MAGIC # mobility Bronze Table
# MAGIC 
# MAGIC The goal of this script is to create mobility Bronze table 
# MAGIC 
# MAGIC The following tables are read:
# MAGIC 
# MAGIC | Table |
# MAGIC | ------ |
# MAGIC | 'https://storage.googleapis.com/covid19-open-data/v3/mobility.csv' |
# MAGIC 
# MAGIC 
# MAGIC The following tables are created:
# MAGIC 
# MAGIC | Tables |
# MAGIC | ------ |
# MAGIC | 'bronze_mobility'|

# COMMAND ----------

import pyspark.sql.types as T
import pyspark.sql.functions as F
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
bronze_datalake_location = "dbfs:/FileStore/RAJAT/BRONZE/mobility"
checkpoint_path= "dbfs:/FileStore/RAJAT/CONFIG/autoloader_checkpoints/mobility"
raw_table_path = "https://storage.googleapis.com/covid19-open-data/v3/mobility.csv"
bronze_database = "rajat"
bronze_table_name = "bronze_mobility" 
partition_by = "date"
my_path = '/FileStore/RAJAT/BRONZE/mobility'
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
# MAGIC CREATE TABLE IF NOT EXISTS `mobility` (
# MAGIC   `date` date NOT NULL default '09-09-1900',   
# MAGIC   `location_key` varchar(10) NOT NULL default 'NA',       
# MAGIC   `mobility_retail_and_recreation` double NULL,
# MAGIC   `mobility_grocery_and_pharmacy` double NULL,
# MAGIC   `mobility_parks` double NULL,
# MAGIC   `mobility_transit_stations` double NULL,    
# MAGIC   `mobility_workplaces` double NULL,
# MAGIC   `mobility_residential` double NULL,
# MAGIC );

# COMMAND ----------

# MAGIC %md
# MAGIC ## Read Tables

# COMMAND ----------

logger.info(f'reading source tables ...')
data = pandas.read_csv(raw_table_path)
df = spark.createDataFrame(data)
logger.info(f'row count : {df.count()}')

# COMMAND ----------

try:
    cf = spark.table('rajat.mobility')
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
                condition = F.expr(f""" Hot.date = New.date AND 
                                        Hot.location_key = New.location_key """)
            )
            .whenMatchedUpdate(set =
                {
                  "date": "source.date",
                  "location_key" : "source.location_key",
                  "mobility_retail_and_recreation": "source.mobility_retail_and_recreation",
                  "mobility_grocery_and_pharmacy": "source.mobility_grocery_and_pharmacy",
                  "mobility_parks": "source.mobility_parks",
                  "mobility_transit_stations": "source.mobility_transit_stations",
                  "mobility_workplaces": "source.mobility_workplaces",
                  "mobility_residential" : "source.mobility_residential"
                }
             )
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
