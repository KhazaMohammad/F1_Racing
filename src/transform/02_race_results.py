# Databricks notebook source
# MAGIC %run "./config"

# COMMAND ----------

# Races
# circuits
# drivers
# constructors
# results

# COMMAND ----------

races_df = spark.sql("""select * from `parquet`.`/mnt/processed_databricksstoragef1/races`""")

# COMMAND ----------

races_df.printSchema()

# COMMAND ----------

circuits_df = spark.sql("""select * from `parquet`.`/mnt/processed_databricksstoragef1/circuits`""")

# COMMAND ----------

circuits_df.printSchema()

# COMMAND ----------

drivers_df = spark.sql("""select * from `parquet`.`/mnt/processed_databricksstoragef1/drivers`""")

# COMMAND ----------

drivers_df.printSchema()

# COMMAND ----------

constructors_df = spark.sql("""select * from `parquet`.`/mnt/processed_databricksstoragef1/constructors`""")

# COMMAND ----------

constructors_df.printSchema()

# COMMAND ----------

results_df = spark.sql("""select * from `parquet`.`/mnt/processed_databricksstoragef1/results`""")

# COMMAND ----------

results_df.printSchema()

# COMMAND ----------

from pyspark.sql.functions import col

# COMMAND ----------

df_joi_races_circuits = circuits_df.join(races_df, circuits_df.circuit_id == races_df.circuit_id , "inner") \
    .select(races_df["race_id"], races_df["race_year"], races_df["name"].alias("race_name"), races_df["date"].alias("race_date") , circuits_df["location"].alias("circuit_location"))

display(df_joi_races_circuits)

# COMMAND ----------

df_joi_results_constructors = results_df.join(constructors_df, results_df.constructor_id == constructors_df.constructor_id , "inner") \
    .select(results_df["race_id"], results_df["constructor_id"], results_df["result_id"], results_df["driver_id"], results_df["grid"], results_df["fastest_lap"], results_df["time"].alias("race_time"), results_df["points"], results_df["position"], constructors_df["name"].alias("team"))

display(df_joi_results_constructors)

# COMMAND ----------

df_joi_results_constructors_drivers = df_joi_results_constructors.join(drivers_df, df_joi_results_constructors.driver_id == drivers_df.driver_id , "inner") \
    .select(df_joi_results_constructors["*"], drivers_df["name"].alias("driver_name"), drivers_df["number"].alias("driver_number"), drivers_df["nationality"].alias("driver_nationality"))

df_joi_results_constructors_drivers.display()

# COMMAND ----------

df_joi_races_circuits_results_constructors_drivers = df_joi_races_circuits.join(df_joi_results_constructors_drivers, df_joi_races_circuits.race_id==df_joi_results_constructors_drivers.race_id, "inner") \
    .drop(
    df_joi_results_constructors_drivers["race_id"],
    df_joi_results_constructors_drivers["constructor_id"],
    df_joi_results_constructors_drivers["result_id"], 
    df_joi_results_constructors_drivers["driver_id"], 
    df_joi_races_circuits["race_id"]
).sort(df_joi_races_circuits["race_name"], df_joi_results_constructors_drivers["points"].desc())
    
display(df_joi_races_circuits_results_constructors_drivers)

# COMMAND ----------

df_joi_races_circuits_results_constructors_drivers.write.format("parquet").mode("overwrite").save(f"{presentaion_folder_path}/race_results")