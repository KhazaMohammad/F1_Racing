# Databricks notebook source
# MAGIC %run "../config"

# COMMAND ----------

from pyspark.sql.functions import col, current_date, current_timestamp, concat, concat_ws, to_timestamp, lit, explode
from pyspark.sql.types import *

# COMMAND ----------

pits_schema = StructType([
    StructField("driverId",IntegerType(),True),
    StructField("duration",StringType(),True),
    StructField("lap",DoubleType(), True),
    StructField("milliseconds",DoubleType(), True),
    StructField("raceId",IntegerType(),True),
    StructField("stop",IntegerType(), True),
    StructField("time",StringType(), True)
])

# COMMAND ----------

df_pits = spark.read \
    .format("json") \
    .schema(pits_schema) \
    .option("multiline", True) \
    .load(f"dbfs:{raw_folder_path}/pit_stops.json")
display(df_pits)

# COMMAND ----------

df_pits_raw = df_pits \
                    .select(col("raceId").alias("race_id"),
                            col("driverId").alias("driver_id"),
                            col("stop"),
                            col("lap"),
                            col("time"),col("duration"),
                            col("milliseconds")
                            ) \
                    .withColumn("data_source",lit(v_data_source)) \
                    .withColumn("Ingestion_date", current_timestamp())
df_pits_raw = add_ingestion_date(df_pits_raw)
display(df_pits_raw)

# COMMAND ----------

df_pits_raw.write.format("parquet").mode("overwrite").parquet("dbfs:/mnt/processed_databricksstoragef1/pit_stops")