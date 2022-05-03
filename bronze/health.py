# Databricks notebook source
# MAGIC %md
# MAGIC # health Bronze Table
# MAGIC 
# MAGIC The goal of this script is to create health Bronze table 
# MAGIC 
# MAGIC The following tables are read:
# MAGIC 
# MAGIC | Table |
# MAGIC | ------ |
# MAGIC | 'https://storage.googleapis.com/covid19-open-data/v3/health.csv' |
# MAGIC 
# MAGIC 
# MAGIC The following tables are created:
# MAGIC 
# MAGIC | Tables |
# MAGIC | ------ |
# MAGIC | 'bronze_health'|

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
bronze_datalake_location = "dbfs:/FileStore/RAJAT/BRONZE/health"
checkpoint_path= "dbfs:/FileStore/RAJAT/CONFIG/autoloader_checkpoints/health"
raw_table_path = "https://storage.googleapis.com/covid19-open-data/v3/health.csv"
bronze_database = "rajat"
bronze_table_name = "bronze_health" 
partition_by = "date"
my_path = '/FileStore/RAJAT/BRONZE/health'
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
# MAGIC CREATE TABLE IF NOT EXISTS `health` (
# MAGIC   `location_key` varchar(10) NOT NULL default 'NA',       
# MAGIC   `life_expectancy` double NULL,
# MAGIC   `smoking_prevalence` double NULL,
# MAGIC   `diabetes_prevalence` double NULL,
# MAGIC   `infant_mortality_rate` double NULL,    
# MAGIC   `adult_male_mortality_rate` double NULL,
# MAGIC   `adult_female_mortality_rate` double NULL,
# MAGIC   `pollution_mortality_rate` double NULL,
# MAGIC   `comorbidity_mortality_rate` double NULL,
# MAGIC   `hospital_beds_per_1000` double NULL,
# MAGIC   `nurses_per_1000` double NULL,    
# MAGIC   `physicians_per_1000` double NULL,
# MAGIC   `health_expenditure_usd` double NULL,
# MAGIC   `out_of_pocket_health_expenditure_usd` double NULL,
# MAGIC );

# COMMAND ----------

# MAGIC %md
# MAGIC ## Reading from source

# COMMAND ----------

logger.info(f'reading source tables ...')
data = pandas.read_csv(raw_table_path)
df = spark.createDataFrame(data)
logger.info(f'row count : {df.count()}')

# COMMAND ----------

try:
    cf = spark.table('rajat.health')
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
                condition = F.expr(f""" Hot.life_expectancy = New.life_expectancy AND 
                                        Hot.location_key = New.location_key """)
            )
            .whenMatchedUpdate(set =
                {
                  "location_key": "source.location_key",
                  "life_expectancy" : "source.life_expectancy",
                  "smoking_prevalence": "source.smoking_prevalence",
                  "diabetes_prevalence": "source.diabetes_prevalence",
                  "infant_mortality_rate": "source.infant_mortality_rate",
                  "adult_male_mortality_rate": "source.adult_male_mortality_rate",
                  "adult_female_mortality_rate": "source.adult_female_mortality_rate",
                  "comorbidity_mortality_rate": "source.comorbidity_mortality_rate",
                  "hospital_beds_per_1000" : "source.hospital_beds_per_1000",
                  "nurses_per_1000" : "source.nurses_per_1000",
                  "physicians_per_1000" : "source.physicians_per_1000",
                  "health_expenditure_usd" : "source.health_expenditure_usd",
                  "out_of_pocket_health_expenditure_usd" : "source.out_of_pocket_health_expenditure_usd"
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
