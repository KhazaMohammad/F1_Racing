# Databricks notebook source
# MAGIC %run "../config"

# COMMAND ----------

from pyspark.sql.functions import col, current_date, current_timestamp, concat, concat_ws, to_timestamp, lit, explode
from pyspark.sql.types import *

# COMMAND ----------

results_schema = StructType([
    StructField("constructorId",DoubleType(),False),
    StructField("driverId",DoubleType(),True),
    StructField("fastestLap",StringType(), True),
    StructField("fastestLapSpeed",StringType(), True),
    StructField("fastestLapTime",StringType(),True),
    StructField("grid",DoubleType(), True),
    StructField("laps",DoubleType(), True),
    StructField("milliseconds",StringType(), True),
    StructField("number",StringType(),True),
    StructField("points",DoubleType(),True),
    StructField("position",StringType(), True),
    StructField("positionOrder",DoubleType(), True),
    StructField("positionText",StringType(),True),
    StructField("raceId",DoubleType(), True),
    StructField("rank",StringType(), True),
    StructField("resultId",DoubleType(), True),
    StructField("statusId",DoubleType(), True),
    StructField("time",StringType(), True)
])

# COMMAND ----------

df_results = spark.read \
    .format("json") \
    .option("Header", True) \
    .schema(results_schema) \
    .load(f"dbfs:{raw_folder_path}/results.json")
display(df_results)

# COMMAND ----------

df_results_raw = df_results \
                    .select(col("resultId").alias("result_id"), 
                            col("raceId").alias("race_id"),
                            col("driverId").alias("driver_id"),
                            col("constructorId").alias("constructor_id"),
                            col("number"),col("grid"),col("position"),
                            col("positionText").alias("position_text"),
                            col("positionOrder").alias("position_order"),
                            col("points"),col("laps"),col("time"),col("milliseconds"),
                            col("fastestLap").alias("fastest_lap"),
                            col("rank"),
                            col("fastestLapTime").alias("fastest_lap_time"),
                            col("fastestLapSpeed").alias("fastest_lap_speed")
                            ) \
                    .withColumn("data_source",lit(v_data_source)) \
                    .withColumn("Ingestion_date", current_timestamp())
df_results_raw = add_ingestion_date(df_results_raw)
display(df_results_raw)

# COMMAND ----------

df_results_raw.write.format("parquet").mode("overwrite").parquet("dbfs:/mnt/processed_databricksstoragef1/results")