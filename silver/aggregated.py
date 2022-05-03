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
import smtplib
import pandas

# COMMAND ----------

#General Configuration
script_execution_start_time = dt.datetime.now()
app_name = 'covid-19'
silver_datalake_location = "dbfs:/FileStore/RAJAT/BRONZE/aggergated"
silver_database = "rajat"
silver_table_name = "silver_aggregated" 
partition_by = "location_key"
notebook_name = dbutils.notebook.entry_point.getDbutils().notebook().getContext().notebookPath().get()
logger = logging.getLogger(app_name)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Read Tables

# COMMAND ----------

logger.info(f'reading source table: rajat.bronze_demographics')
demographics_df = spark.table('rajat.bronze_demographics')

logger.info(f'reading source table: rajat.bronze_epidemiology')
epidemiology_df = spark.table('rajat.bronze_epidemiology')

logger.info(f'reading source table: rajat.bronze_hospitalizations')
hospitalizations_df = spark.table('rajat.bronze_hospitalizations')

logger.info(f'reading source table: rajat.bronze_health')
health_df = spark.table('rajat.bronze_health')

logger.info(f'reading source table: rajat.bronze_mobility')
mobility_df = spark.table('rajat.bronze_mobility')

logger.info(f'reading source table: rajat.bronze_vaccinations')
vaccinations_df = spark.table('rajat.bronze_vaccinations')

# COMMAND ----------

# MAGIC %md
# MAGIC ## Applying transformation

# COMMAND ----------

logger.info(f'applying transformation ')
df = epidemiology_df.join(hospitalizations_df, ['location_key', 'date'], "left")\
                    .join(mobility_df, ['location_key', 'date'], "left")\
                    .join(vaccinations_df, ['location_key', 'date'], "left")
logger.info(f'df count after transformation: {df.count()}')

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
            source = df.alias("New"),
            condition = F.expr(f""" Hot.date = New.date
                                    AND Hot.location_key = New.location_key
                                """)
        )
        .whenMatchedUpdate(set = {
            'location_key' :   F.col('New.location_key'),                                   
            'date' :   F.col('New.date'),                                  
            'new_confirmed' :  F.col('New.new_confirmed'),                                      
            'new_deceased' :  F.col('New.new_deceased'),                                   
            'new_recovered' :  F.col('New.new_recovered'),                                      
            'new_tested' :  F.col('New.new_tested'),                                     
            'cumulative_confirmed' :  F.col('New.cumulative_confirmed'),                                   
            'cumulative_deceased' :  F.col('New.cumulative_deceased'),                                    
            'cumulative_recovered' :  F.col('New.cumulative_recovered'),                                   
            'cumulative_tested' :   F.col('New.cumulative_tested'),                                     
            'new_hospitalized_patients' : F.col('New.new_hospitalized_patients'),                                       
            'cumulative_hospitalized_patients' : F.col('New.cumulative_hospitalized_patients'),                                    
            'current_hospitalized_patients' :  F.col('New.current_hospitalized_patients'),                                      
            'new_intensive_care_patients' :  F.col('New.new_intensive_care_patients'),                                    
            'cumulative_intensive_care_patients' : F.col('New.cumulative_intensive_care_patients'),                                      
            'current_intensive_care_patients' :  F.col('New.current_intensive_care_patients'),                                    
            'new_ventilator_patients' :  F.col('New.new_ventilator_patients'),                                    
            'cumulative_ventilator_patients' : F.col('New.cumulative_ventilator_patients'),                                      
            'current_ventilator_patients' :  F.col('New.current_ventilator_patients'),                                    
            'mobility_retail_and_recreation' : F.col('New.mobility_retail_and_recreation'),                                      
            'mobility_grocery_and_pharmacy' :  F.col('New.mobility_grocery_and_pharmacy'),                                      
            'mobility_parks' :  F.col('New.mobility_parks'),                                     
            'mobility_transit_stations' : F.col('New.mobility_transit_stations'),                                       
            'mobility_workplaces' :  F.col('New.mobility_workplaces'),                                    
            'mobility_residential' : F.col('New.mobility_residential'),                                    
            'new_persons_vaccinated' : F.col('New.new_persons_vaccinated'),                                      
            'cumulative_persons_vaccinated' : F.col('New.cumulative_persons_vaccinated'),                                       
            'new_persons_fully_vaccinated' :  F.col('New.new_persons_fully_vaccinated'),                                   
            'cumulative_persons_fully_vaccinated' : F.col('New.cumulative_persons_fully_vaccinated'),                                     
            'new_vaccine_doses_administered' :  F.col('New.new_vaccine_doses_administered'),                                     
            'cumulative_vaccine_doses_administered' : F.col('New.cumulative_vaccine_doses_administered'),                                       
            'new_persons_vaccinated_pfizer' :  F.col('New.new_persons_vaccinated_pfizer'),                                      
            'cumulative_persons_vaccinated_pfizer' : F.col('New.cumulative_persons_vaccinated_pfizer'),                                    
            'new_persons_fully_vaccinated_pfizer' :  F.col('New.new_persons_fully_vaccinated_pfizer'),                                    
            'cumulative_persons_fully_vaccinated_pfizer' : F.col('New.cumulative_persons_fully_vaccinated_pfizer'),                                      
            'new_vaccine_doses_administered_pfizer' :  F.col('New.new_vaccine_doses_administered_pfizer'),                                      
            'cumulative_vaccine_doses_administered_pfizer' :  F.col('New.cumulative_vaccine_doses_administered_pfizer'),                                   
            'new_persons_vaccinated_moderna' : F.col('New.new_persons_vaccinated_moderna'),                                      
            'cumulative_persons_vaccinated_moderna' : F.col('New.cumulative_persons_vaccinated_moderna'),                                       
            'new_persons_fully_vaccinated_moderna' :  F.col('New.new_persons_fully_vaccinated_moderna'),                                   
            'cumulative_persons_fully_vaccinated_moderna' :  F.col('New.cumulative_persons_fully_vaccinated_moderna'),                                    
            'new_vaccine_doses_administered_moderna' :  F.col('New.new_vaccine_doses_administered_moderna'),                                     
            'cumulative_vaccine_doses_administered_moderna' : F.col('New.cumulative_vaccine_doses_administered_moderna'),                                        
            'new_persons_vaccinated_janssen' :  F.col('New.new_persons_vaccinated_janssen'),                                     
            'cumulative_persons_vaccinated_janssen' :  F.col('New.cumulative_persons_vaccinated_janssen'),                                      
            'new_persons_fully_vaccinated_janssen' :  F.col('New.new_persons_fully_vaccinated_janssen'),                                   
            'cumulative_persons_fully_vaccinated_janssen' :  F.col('New.cumulative_persons_fully_vaccinated_janssen'),                                    
            'new_vaccine_doses_administered_janssen' :  F.col('New.new_vaccine_doses_administered_janssen'),                                     
            'cumulative_vaccine_doses_administered_janssen' : F.col('New.cumulative_vaccine_doses_administered_janssen'),                                       
            'new_persons_vaccinated_sinovac' :  F.col('New.new_persons_vaccinated_sinovac'),                                     
            'total_persons_vaccinated_sinovac' : F.col('New.total_persons_vaccinated_sinovac'),                                    
            'new_persons_fully_vaccinated_sinovac' :  F.col('New.new_persons_fully_vaccinated_sinovac'),                                   
            'total_persons_fully_vaccinated_sinovac' : F.col('New.total_persons_fully_vaccinated_sinovac'),                                      
            'new_vaccine_doses_administered_sinovac' :  F.col('New.new_vaccine_doses_administered_sinovac'),                                     
            'total_vaccine_doses_administered_sinovac' : F.col('New.total_vaccine_doses_administered_sinovac')                                    
        })
        .whenNotMatchedInsertAll()
    ).execute()
    logging.info(f'Finished merging data to Silver Layer')

# COMMAND ----------

logging.info('writing data in bigquery')
(df.write.format("bigquery")
  .option('project_id', 'rekitt-training-cloud')
  .option("table","rekitt-training-cloud.rajat_kashyap.aggregated")
  .save())

# COMMAND ----------

script_execution_end_time = dt.datetime.now()

final_output = {
                'notebook_name': notebook_name, 
                'start_timestamp': script_execution_start_time.strftime('%Y-%m-%d %H:%m:%S'), 
                'end_timestamp': script_execution_end_time.strftime('%Y-%m-%d %H:%m:%S'),
                'duration': str(script_execution_end_time - script_execution_start_time),
               }

# COMMAND ----------

dbutils.notebook.exit('final_output')
