# Databricks notebook source
# MAGIC %md
# MAGIC # hospitalizations Bronze Table
# MAGIC 
# MAGIC The goal of this script is to create hospitalizations Bronze table 
# MAGIC 
# MAGIC The following tables are read:
# MAGIC 
# MAGIC | Table |
# MAGIC | ------ |
# MAGIC | 'https://storage.googleapis.com/covid19-open-data/v3/hospitalizations.csv' |
# MAGIC 
# MAGIC 
# MAGIC The following tables are created:
# MAGIC 
# MAGIC | Tables |
# MAGIC | ------ |
# MAGIC | 'bronze_hospitalizations'|

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
bronze_datalake_location = "dbfs:/FileStore/RAJAT/BRONZE/hospitalizations"
checkpoint_path= "dbfs:/FileStore/RAJAT/CONFIG/autoloader_checkpoints/hospitalizations"
raw_table_path = "https://storage.googleapis.com/covid19-open-data/v3/hospitalizations.csv"
bronze_database = "rajat"
bronze_table_name = "bronze_hospitalizations" 
partition_by = "date"
my_path = '/FileStore/RAJAT/BRONZE/hospitalizations'
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
# MAGIC CREATE TABLE IF NOT EXISTS `hospitalizations` (
# MAGIC   `date` date NOT NULL default '09-09-1900',   
# MAGIC   `location_key` varchar(10) NOT NULL default 'NA',          
# MAGIC   `new_hospitalized_patients` integer(255) NULL,
# MAGIC   `cumulative_hospitalized_patients` integer(255) NULL,
# MAGIC   `current_hospitalized_patients` integer(255) NULL,
# MAGIC   `new_intensive_care_patients` integer(255) NULL,    
# MAGIC   `cumulative_intensive_care_patients` integer(255) NULL,
# MAGIC   `current_intensive_care_patients` integer(255) NULL,
# MAGIC   `new_ventilator_patients` integer(255) NULL,
# MAGIC   `cumulative_ventilator_patients` integer(255) NULL,
# MAGIC   `current_ventilator_patients` integer(255) NULL,
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
    cf = spark.table('rajat.hospitalizations')
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
              "new_hospitalized_patients": "source.new_hospitalized_patients",
              "cumulative_hospitalized_patients": "source.cumulative_hospitalized_patients",
              "current_hospitalized_patients": "source.current_hospitalized_patients",
              "new_intensive_care_patients": "source.new_intensive_care_patients",
              "cumulative_intensive_care_patients": "source.cumulative_intensive_care_patients",
              "current_intensive_care_patients": "source.current_intensive_care_patients",
              "new_ventilator_patients" : "source.new_ventilator_patients",
              "cumulative_ventilator_patients" : "source.cumulative_ventilator_patients",
              "current_ventilator_patients" : "source.current_ventilator_patients"
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
