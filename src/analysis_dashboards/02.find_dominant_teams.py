# Databricks notebook source
# MAGIC %run "../config"

# COMMAND ----------

race_results_df = spark.read.parquet("/mnt/presentation_databricksstoragef1/race_results")
race_results_df.createOrReplaceTempView("race_results_sql")

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from race_results_sql limit 10

# COMMAND ----------

# MAGIC %sql
# MAGIC select team as TEAM,
# MAGIC count(1) as total_races,
# MAGIC SUM(points) as total_points,
# MAGIC AVG(points) as average_points
# MAGIC  from race_results_sql
# MAGIC where race_year between 2014 and 2025
# MAGIC group by TEAM
# MAGIC having count(1) >= 50
# MAGIC order by average_points desc

# COMMAND ----------

# MAGIC %sql
# MAGIC select team, 
# MAGIC count(1) as total_races,
# MAGIC SUM(points) as total_points,
# MAGIC AVG(points) as average_points
# MAGIC  from race_results_sql
# MAGIC where race_year between 2004 and 2015
# MAGIC group by team
# MAGIC having count(1) >= 50
# MAGIC order by average_points desc