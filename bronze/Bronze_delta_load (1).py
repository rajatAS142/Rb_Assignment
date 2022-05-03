# Databricks notebook source
import pandas as pd
from pandas import *
from datetime import date,timedelta
import pyspark.sql.functions as f
from delta.tables import *
#ter_csv = pd.read_csv('file.csv', iterator=True, chunksize=10000)
#df = pd.concat([chunk[chunk['date'] >= yesterday] for chunk in iter_csv])
epidemiology_url="https://storage.googleapis.com/covid19-open-data/v3/latest/epidemiology.csv"
demographics_url="https://storage.googleapis.com/covid19-open-data/v3/latest/demographics.csv"
health_url="https://storage.googleapis.com/covid19-open-data/v3/latest/health.csv"
hospitalizations_url="https://storage.googleapis.com/covid19-open-data/v3/latest/hospitalizations.csv"
mobility_url="https://storage.googleapis.com/covid19-open-data/v3/latest/mobility.csv"
vaccinations_url="https://storage.googleapis.com/covid19-open-data/v3/latest/vaccinations.csv"


epidemiology_c=pd.read_csv(epidemiology_url)
epidemiology_sparkDF=spark.createDataFrame(epidemiology_c)

mobility_c=pd.read_csv(mobility_url)
mobility_sparkDF=spark.createDataFrame(mobility_c)

demographics_c=pd.read_csv(demographics_url)
demographics_sparkDF=spark.createDataFrame(demographics_c)

health_c=pd.read_csv(health_url)
health_sparkDF=spark.createDataFrame(health_c)

hospitalizations_c=pd.read_csv(hospitalizations_url)
hospitalizations_sparkDF=spark.createDataFrame(hospitalizations_c)

vaccinations_c=pd.read_csv(vaccinations_url)
vaccinations_sparkDF=spark.createDataFrame(vaccinations_c)

#datF=data.select(*epidemiology_col)
#print(data.collect()[0][0])
#epidemiology_col = pandas.read_csv("https://storage.googleapis.com/covid19-open-data/v3/epidemiology.csv",header=None,nrows=1).iloc[0]
#dfffs= data.select(*epidemiology_col)
#dfffs.count()
#datF.show(40000)
#datF.count()



# COMMAND ----------


deltaDemographics= DeltaTable.forPath(spark, '/FileStore/lovedeep/lovedeep_bronze/demographics')


deltaDemographics.alias("target").merge(demographics_sparkDF.alias("source"),'target.location_key=source.location_key') \
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
        
    }
  ) \
  .whenNotMatchedInsert(values =
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
    }
  ) \
  .execute()


# COMMAND ----------


deltaEpidemiology= DeltaTable.forPath(spark, '/FileStore/lovedeep/lovedeep_bronze/epidemiology')


deltaEpidemiology.alias("target").merge(epidemiology_sparkDF.alias("source"),'target.date == source.date and target.location_key=source.location_key') \
  .whenMatchedUpdate(set =
    {
      "date": "source.date",
      "location_key" : "source.location_key",
      "new_confirmed": "source.new_confirmed",
      "new_deceased": "source.new_deceased",
      "new_recovered": "source.new_recovered",
      "new_tested": "source.new_tested",
      "cumulative_confirmed": "source.cumulative_confirmed",
      "cumulative_deceased" : "source.cumulative_deceased",
      "cumulative_recovered" : "source.cumulative_recovered",
      "cumulative_tested" : "source.cumulative_tested"
    }
  ) \
  .whenNotMatchedInsert(values =
    {
      "date": "source.date",
      "location_key": "source.location_key",
      "new_confirmed": "source.new_confirmed",
      "new_deceased": "source.new_deceased",
      "new_recovered": "source.new_recovered",
      "new_tested": "source.new_tested",
      "cumulative_confirmed": "source.cumulative_confirmed",
      "cumulative_deceased": "source.cumulative_deceased",
      "cumulative_recovered": "source.cumulative_recovered",
      "cumulative_tested": "source.cumulative_tested"
    }
  ) \
  .execute()

# COMMAND ----------


deltaHealth= DeltaTable.forPath(spark, '/FileStore/lovedeep/lovedeep_bronze/health')


deltaHealth.alias("target").merge(health_sparkDF.alias("source"),'target.location_key=source.location_key') \
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
  ) \
  .whenNotMatchedInsert(values =
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
  ) \
  .execute()

# COMMAND ----------


deltahospitalizations= DeltaTable.forPath(spark, '/FileStore/lovedeep/lovedeep_bronze/hospitalizations')


deltahospitalizations.alias("target").merge(hospitalizations_sparkDF.alias("source"),'target.date == source.date and target.location_key=source.location_key') \
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
  ) \
  .whenNotMatchedInsert(values =
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
  ) \
  .execute()

# COMMAND ----------


deltamobility= DeltaTable.forPath(spark, '/FileStore/lovedeep/lovedeep_bronze/mobility')


deltamobility.alias("target").merge(mobility_sparkDF.alias("source"),'target.date == source.date and target.location_key=source.location_key') \
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
  ) \
  .whenNotMatchedInsert(values =
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
  ) \
  .execute()

# COMMAND ----------

deltavaccinations= DeltaTable.forPath(spark, '/FileStore/lovedeep/lovedeep_bronze/vaccinations')


deltavaccinations.alias("target").merge(vaccinations_sparkDF.alias("source"),'target.date == source.date and target.location_key=source.location_key') \
  .whenMatchedUpdate(set =
    {
      "date": "source.date",
      "location_key" : "source.location_key",
      "new_persons_vaccinated": "source.new_persons_vaccinated",
      "cumulative_persons_vaccinated": "source.cumulative_persons_vaccinated",
      "new_persons_fully_vaccinated": "source.new_persons_fully_vaccinated",
      "cumulative_persons_fully_vaccinated": "source.cumulative_persons_fully_vaccinated",
      "new_vaccine_doses_administered": "source.new_vaccine_doses_administered",
      "cumulative_vaccine_doses_administered" : "source.cumulative_vaccine_doses_administered"
    }
  ) \
  .whenNotMatchedInsert(values =
    {
      "date": "source.date",
      "location_key" : "source.location_key",
      "new_persons_vaccinated": "source.new_persons_vaccinated",
      "cumulative_persons_vaccinated": "source.cumulative_persons_vaccinated",
      "new_persons_fully_vaccinated": "source.new_persons_fully_vaccinated",
      "cumulative_persons_fully_vaccinated": "source.cumulative_persons_fully_vaccinated",
      "new_vaccine_doses_administered": "source.new_vaccine_doses_administered",
      "cumulative_vaccine_doses_administered" : "source.cumulative_vaccine_doses_administered"
    }
  ) \
  .execute()

# COMMAND ----------


