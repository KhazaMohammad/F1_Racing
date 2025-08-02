# Databricks notebook source
# MAGIC %run "../config"

# COMMAND ----------

from pyspark.sql.functions import col, current_date, current_timestamp, concat, concat_ws, to_timestamp, lit, explode
from pyspark.sql.types import *

# COMMAND ----------

lap_times_schema = StructType([
    StructField("raceId",IntegerType(),True),
    StructField("driverId",StringType(),True),
    StructField("lap",DoubleType(), True),
    StructField("position",IntegerType(), True),
    StructField("time",StringType(), True),
    StructField("milliseconds",IntegerType(), True)
])

# COMMAND ----------

df_lap_times = spark.read \
    .format("csv") \
    .schema(lap_times_schema) \
    .load(f"dbfs:{raw_folder_path}/lap_times/lap_times_split*.csv")
display(df_lap_times)

# COMMAND ----------

df_lap_times_raw = df_lap_times \
                    .select(col("raceId").alias("race_id"),
                            col("driverId").alias("driver_id"),
                            col("lap"),
                            col("position"),
                            col("time"),
                            col("milliseconds")
                            ) \
                    .withColumn("data_source",lit(v_data_source)) \
                    .withColumn("Ingestion_date", current_timestamp())
df_lap_times_raw = add_ingestion_date(df_lap_times_raw)
display(df_lap_times_raw)

# COMMAND ----------

df_lap_times_raw.write.format("parquet").mode("overwrite").parquet("dbfs:/mnt/processed_databricksstoragef1/lap_times")